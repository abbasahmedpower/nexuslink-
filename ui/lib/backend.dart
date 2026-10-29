import 'dart:async';
import 'dart:convert';
import 'dart:io';

/// Represents a device known to the hub.
class DeviceInfo {
  final String id;
  final String name;
  final String origin;
  final bool paired;
  final bool reachable;
  final String? address;

  const DeviceInfo({
    required this.id,
    required this.name,
    required this.origin,
    required this.paired,
    required this.reachable,
    this.address,
  });

  factory DeviceInfo.fromJson(Map<String, dynamic> json) => DeviceInfo(
        id: json['id'] as String,
        name: json['name'] as String,
        origin: json['origin'] as String,
        paired: json['paired'] as bool? ?? false,
        reachable: json['reachable'] as bool? ?? false,
        address: json['address'] as String?,
      );
}

/// Represents an active or completed streaming session.
class StreamSession {
  final String sessionId;
  final String kind;
  final String? deviceSerial;
  final String state;
  final int startedAtMs;
  final int? pid;
  final int? exitCode;
  final String? lastError;

  const StreamSession({
    required this.sessionId,
    required this.kind,
    this.deviceSerial,
    required this.state,
    required this.startedAtMs,
    this.pid,
    this.exitCode,
    this.lastError,
  });

  factory StreamSession.fromJson(Map<String, dynamic> json) => StreamSession(
        sessionId: json['session_id'] as String,
        kind: json['kind'] as String,
        deviceSerial: json['device_serial'] as String?,
        state: json['state'] as String,
        startedAtMs: (json['started_at_ms'] as num).toInt(),
        pid: json['pid'] as int?,
        exitCode: json['exit_code'] as int?,
        lastError: json['last_error'] as String?,
      );
}

class ToolResult {
  final bool ok;
  final String text;
  final dynamic data;
  const ToolResult(this.ok, this.text, [this.data]);
}

/// Validates device identifiers strictly to prevent argument injection
bool validDeviceId(String id) {
  if (id.isEmpty || id.length > 128 || id.startsWith('-')) return false;
  return RegExp(r'^[A-Za-z0-9_.:-]+$').hasMatch(id);
}

/// Typed client for Rust Hub IPC daemon over authenticated local loopback transport.
class NexusIpcClient {
  final String host;
  final int port;
  Socket? _socket;
  StreamSubscription? _sub;
  int _seq = 1;
  final Map<int, Completer<Map<String, dynamic>>> _pending = {};
  final StreamController<Map<String, dynamic>> _events =
      StreamController<Map<String, dynamic>>.broadcast();

  NexusIpcClient({this.host = '127.0.0.1', this.port = 8788});

  bool get isConnected => _socket != null;
  Stream<Map<String, dynamic>> get events => _events.stream;

  static String? readAuthToken() {
    try {
      final env = Platform.environment;
      String runDirPath;
      if (Platform.isWindows) {
        final localAppData = env['LOCALAPPDATA'];
        if (localAppData != null) {
          runDirPath = '$localAppData\\NexusHub\\run';
        } else {
          final profile = env['USERPROFILE'] ?? '';
          runDirPath = '$profile\\.nexus_hub\\run';
        }
      } else {
        final xdg = env['XDG_RUNTIME_DIR'];
        if (xdg != null) {
          // Match Rust: $XDG_RUNTIME_DIR/nexus_hub/
          runDirPath = '$xdg/nexus_hub';
        } else {
          final home = env['HOME'] ?? '';
          // Match Rust: $HOME/.nexus_hub/run
          runDirPath = '$home/.nexus_hub/run';
        }
      }
      final file = File('$runDirPath${Platform.pathSeparator}nexus_ipc.token');
      if (file.existsSync()) {
        return file.readAsStringSync().trim();
      }
    } catch (_) {}
    return null;
  }

  Future<void> connect() async {
    if (_socket != null) return;
    _socket = await Socket.connect(host, port, timeout: const Duration(seconds: 3));
    _socket!.transform(utf8.decoder).transform(const LineSplitter()).listen(
      _onLine,
      onError: (err) {
        _cleanup();
      },
      onDone: () {
        _cleanup();
      },
    );

    // Perform Handshake
    final token = readAuthToken();
    final res = await _call('handshake', {
      'protocol_version': 1,
      'client_name': 'flutter-desktop-ui',
      if (token != null) 'auth_token': token,
    });

    if (res['status'] != 'ok') {
      final err = res['error']?['message'] ?? 'Authentication failed';
      _cleanup();
      throw Exception('IPC handshake rejected: $err');
    }

    // Subscribe to daemon events
    unawaited(_call('subscribe_events', null));
  }

  void _cleanup() {
    _socket?.destroy();
    _socket = null;
    for (final completer in _pending.values) {
      if (!completer.isCompleted) {
        completer.completeError(Exception('IPC connection closed'));
      }
    }
    _pending.clear();
  }

  void _onLine(String line) {
    if (line.trim().isEmpty) return;
    try {
      final decoded = jsonDecode(line) as Map<String, dynamic>;
      final id = decoded['id'] as int?;
      if (id != null && id != 0) {
        final completer = _pending.remove(id);
        completer?.complete(decoded);
      } else if (decoded.containsKey('event')) {
        _events.add(decoded);
      }
    } catch (_) {}
  }

  Future<Map<String, dynamic>> _call(String action, dynamic payload) async {
    if (_socket == null) {
      await connect();
    }
    final id = _seq++;
    final completer = Completer<Map<String, dynamic>>();
    _pending[id] = completer;

    final req = jsonEncode({
      'id': id,
      'action': action,
      if (payload != null) 'payload': payload,
    });

    _socket!.write('$req\n');

    return completer.future.timeout(
      const Duration(seconds: 15),
      onTimeout: () {
        _pending.remove(id);
        throw TimeoutException('Request timed out: $action');
      },
    );
  }

  Future<ToolResult> getCapabilities() async {
    try {
      final res = await _call('get_capabilities', null);
      if (res['status'] == 'ok') {
        return ToolResult(true, 'Capabilities received', res['data']);
      }
      return ToolResult(false, res['error']?['message'] ?? 'Unknown error');
    } catch (e) {
      return ToolResult(false, 'Daemon communication error: $e');
    }
  }

  Future<List<dynamic>> getDiscoveryPeers() async {
    try {
      final res = await _call('get_discovery_peers', null);
      if (res['status'] == 'ok' && res['data'] is List) {
        return res['data'] as List<dynamic>;
      }
    } catch (_) {}
    return [];
  }

  Future<ToolResult> listDevices() async {
    try {
      final res = await _call('list_devices', null);
      if (res['status'] == 'ok') {
        final list = (res['data'] as List<dynamic>?) ?? [];
        final parsed = list.map((item) => DeviceInfo.fromJson(item as Map<String, dynamic>)).toList();
        final text = parsed.isEmpty
            ? 'No paired KDE devices found.'
            : parsed.map((d) => '${d.id}: ${d.name}').join('\n');
        return ToolResult(true, text, parsed);
      }
      return ToolResult(false, res['error']?['message'] ?? 'Failed to list devices');
    } catch (e) {
      return ToolResult(false, 'Daemon communication error: $e');
    }
  }

  Future<ToolResult> pairDevice(String id) async {
    if (!validDeviceId(id)) return const ToolResult(false, 'Invalid device id');
    try {
      final res = await _call('pair_device', {'device_id': id});
      if (res['status'] == 'ok') {
        return ToolResult(true, (res['data'] as String?) ?? 'Pair requested');
      }
      return ToolResult(false, res['error']?['message'] ?? 'Pair failed');
    } catch (e) {
      return ToolResult(false, 'Daemon communication error: $e');
    }
  }

  Future<ToolResult> pingDevice(String id) async {
    if (!validDeviceId(id)) return const ToolResult(false, 'Invalid device id');
    try {
      final res = await _call('ping_device', {'device_id': id});
      if (res['status'] == 'ok') {
        return ToolResult(true, (res['data'] as String?) ?? 'Ping OK');
      }
      return ToolResult(false, res['error']?['message'] ?? 'Ping failed');
    } catch (e) {
      return ToolResult(false, 'Daemon communication error: $e');
    }
  }

  Future<ToolResult> shareFile(String id, String path) async {
    if (!validDeviceId(id)) return const ToolResult(false, 'Invalid device id');
    if (path.trim().isEmpty) return const ToolResult(false, 'Path cannot be empty');
    try {
      final res = await _call('share_file', {'device_id': id, 'file_or_url': path});
      if (res['status'] == 'ok') {
        return ToolResult(true, (res['data'] as String?) ?? 'File shared');
      }
      return ToolResult(false, res['error']?['message'] ?? 'Share failed');
    } catch (e) {
      return ToolResult(false, 'Daemon communication error: $e');
    }
  }

  Future<ToolResult> startMirror({String? serial, bool camera = false, String? recording}) async {
    if (serial != null && serial.isNotEmpty && !validDeviceId(serial)) {
      return const ToolResult(false, 'Invalid ADB serial');
    }
    try {
      final res = await _call('start_mirror', {
        'serial': (serial != null && serial.isNotEmpty) ? serial : null,
        'camera': camera,
        'record': recording,
      });
      if (res['status'] == 'ok') {
        final sess = StreamSession.fromJson(res['data'] as Map<String, dynamic>);
        return ToolResult(
          true,
          'Started scrcpy (session: ${sess.sessionId}, PID: ${sess.pid ?? "unknown"}). Video opens in a separate native window.',
          sess,
        );
      }
      return ToolResult(false, res['error']?['message'] ?? 'Failed to start scrcpy');
    } catch (e) {
      return ToolResult(false, 'Daemon communication error: $e');
    }
  }

  Future<ToolResult> stopMirror({String? sessionId}) async {
    try {
      final res = await _call('stop_mirror', {
        'session_id': sessionId,
      });
      if (res['status'] == 'ok') {
        return const ToolResult(true, 'Stream stopped successfully');
      }
      return ToolResult(false, res['error']?['message'] ?? 'Failed to stop stream');
    } catch (e) {
      return ToolResult(false, 'Daemon communication error: $e');
    }
  }

  Future<StreamSession?> getSession() async {
    try {
      final res = await _call('get_session', null);
      if (res['status'] == 'ok' && res['data'] != null) {
        return StreamSession.fromJson(res['data'] as Map<String, dynamic>);
      }
    } catch (_) {}
    return null;
  }

  void dispose() {
    _cleanup();
    _events.close();
  }
}
