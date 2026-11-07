import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:flutter/foundation.dart';

/// Manages connection lifecycle states to prevent race conditions.
enum IpcConnectionState { disconnected, connecting, connected, failed }

/// Generic, type-safe wrapper for IPC operation results.
class ToolResult<T> {
  final bool success;
  final String message;
  final T? data;
  final Object? error;

  const ToolResult._({required this.success, required this.message, this.data, this.error});

  factory ToolResult.success(String message, [T? data]) =>
      ToolResult._(success: true, message: message, data: data);

  factory ToolResult.failure(String message, [Object? error]) =>
      ToolResult._(success: false, message: message, error: error);
}

/// Safe parsing extensions to prevent runtime crashes on malformed JSON.
extension SafeMap on Map<String, dynamic> {
  String getStr(String key, [String fallback = '']) => this[key] is String ? this[key] as String : fallback;
  int getInt(String key, [int fallback = 0]) => this[key] is int ? this[key] as int : (this[key] is num ? (this[key] as num).toInt() : fallback);
  bool getBool(String key, [bool fallback = false]) => this[key] is bool ? this[key] as bool : fallback;
  Map<String, dynamic>? getMap(String key) => this[key] is Map<String, dynamic> ? this[key] as Map<String, dynamic> : null;
  List<dynamic>? getList(String key) => this[key] is List ? this[key] as List<dynamic> : null;
}

class DeviceInfo {
  final String id, name, origin;
  final bool paired, reachable;
  final String? address;

  const DeviceInfo({
    required this.id, required this.name, required this.origin,
    required this.paired, required this.reachable, this.address,
  });

  factory DeviceInfo.fromJson(Map<String, dynamic> json) => DeviceInfo(
    id: json.getStr('id', 'unknown'),
    name: json.getStr('name', 'Unknown Device'),
    origin: json.getStr('origin', 'unknown'),
    paired: json.getBool('paired'),
    reachable: json.getBool('reachable'),
    address: json.getStr('address').isEmpty ? null : json.getStr('address'),
  );
}

class StreamSession {
  final String sessionId, kind, state;
  final String? deviceSerial, lastError;
  final int startedAtMs;
  final int? pid, exitCode;

  const StreamSession({
    required this.sessionId, required this.kind, required this.state,
    this.deviceSerial, this.lastError, required this.startedAtMs, this.pid, this.exitCode,
  });

  factory StreamSession.fromJson(Map<String, dynamic> json) => StreamSession(
    sessionId: json.getStr('session_id'),
    kind: json.getStr('kind'),
    state: json.getStr('state', 'unknown'),
    deviceSerial: json.getStr('device_serial').isEmpty ? null : json.getStr('device_serial'),
    startedAtMs: json.getInt('started_at_ms', 0),
    pid: json['pid'] is int ? json['pid'] as int : null,
    exitCode: json['exit_code'] is int ? json['exit_code'] as int : null,
    lastError: json.getStr('last_error').isEmpty ? null : json.getStr('last_error'),
  );
}

/// Centralized validation logic to prevent argument injection.
class DeviceValidator {
  static final RegExp _validIdRegex = RegExp(r'^[A-Za-z0-9_.:-]+$');

  static bool isValid(String id) =>
      id.isNotEmpty && id.length <= 128 && !id.startsWith('-') && _validIdRegex.hasMatch(id);
}

/// Robust IPC client with auto-recovery and memory leak prevention.
class NexusIpcClient {
  final String host;
  final int port;

  Socket? _socket;
  IpcConnectionState _state = IpcConnectionState.disconnected;
  Future<void>? _connectingFuture;
  StreamSubscription<String>? _sub;

  int _seq = 1;
  static const int _maxPendingRequests = 500;
  final Map<int, Completer<Map<String, dynamic>>> _pending = {};

  final StreamController<Map<String, dynamic>> _events = StreamController<Map<String, dynamic>>.broadcast(
    onCancel: () => debugPrint('IPC Events stream canceled'),
  );
  final StreamController<IpcConnectionState> _stateController = StreamController<IpcConnectionState>.broadcast();

  NexusIpcClient({this.host = '127.0.0.1', this.port = 8788});

  IpcConnectionState get state => _state;
  bool get isConnected => _state == IpcConnectionState.connected; // Added for test compatibility
  Stream<IpcConnectionState> get stateStream => _stateController.stream;
  Stream<Map<String, dynamic>> get events => _events.stream;

  void _updateState(IpcConnectionState newState) {
    if (_state != newState) {
      _state = newState;
      if (!_stateController.isClosed) _stateController.add(newState);
    }
  }

  static String? _readAuthToken() {
    try {
      final env = Platform.environment;
      final String runDirPath = Platform.isWindows
          ? (env['LOCALAPPDATA'] != null ? '${env['LOCALAPPDATA']}\\NexusHub\\run' : '${env['USERPROFILE'] ?? ''}\\.nexus_hub\\run')
          : (env['XDG_RUNTIME_DIR'] != null ? '${env['XDG_RUNTIME_DIR']}/nexus_hub' : '${env['HOME'] ?? ''}/.nexus_hub/run');

      final file = File('$runDirPath${Platform.pathSeparator}nexus_ipc.token');
      return file.existsSync() ? file.readAsStringSync().trim() : null;
    } catch (e) {
      debugPrint('Token read error: $e');
      return null;
    }
  }

  Future<void> connect() async {
    if (_state == IpcConnectionState.connected) return;
    if (_connectingFuture != null) return _connectingFuture;

    _updateState(IpcConnectionState.connecting);
    _connectingFuture = _doConnect();

    try {
      await _connectingFuture;
    } catch (e) {
      _updateState(IpcConnectionState.failed);
      rethrow;
    } finally {
      _connectingFuture = null;
    }
  }

  Future<void> _doConnect() async {
    _cleanup(keepState: false);

    try {
      final s = await Socket.connect(host, port, timeout: const Duration(seconds: 3));

      _sub = s
          .cast<List<int>>()
          .transform(const Utf8Decoder(allowMalformed: true))
          .transform(const LineSplitter())
          .listen(
            _onLine,
            onError: (err) { debugPrint('IPC Stream Error: $err'); _handleDisconnect(); },
            onDone: () { debugPrint('IPC Stream Closed'); _handleDisconnect(); },
          );

      final token = _readAuthToken();
      final res = await _sendRawRequest(s, 'handshake', {
        'protocol_version': 1,
        'client_name': 'flutter-desktop-ui',
        if (token != null) 'auth_token': token,
      });

      if (res.getStr('status') != 'ok') {
        s.destroy();
        _cleanup(keepState: false);
        throw Exception('Handshake rejected: ${res.getMap('error')?.getStr('message', 'Unknown')}');
      }

      _socket = s;
      _updateState(IpcConnectionState.connected);
      unawaited(_sendRawRequest(s, 'subscribe_events', null));

    } catch (e) {
      _cleanup(keepState: false);
      rethrow;
    }
  }

  void _handleDisconnect() {
    if (_state == IpcConnectionState.disconnected) return;
    _cleanup(keepState: true);

    Future.delayed(const Duration(milliseconds: 500), () {
      if (_state != IpcConnectionState.disconnected) {
        debugPrint('Attempting auto-reconnect...');
        connect().catchError((e) => debugPrint('Auto-reconnect failed: $e'));
      }
    });
  }

  void _cleanup({bool keepState = false}) {
    _sub?.cancel();
    _sub = null;
    _socket?.destroy();
    _socket = null;

    for (final completer in _pending.values) {
      if (!completer.isCompleted) {
        completer.completeError(StateError('IPC connection lost'));
      }
    }
    _pending.clear();

    if (!keepState) _updateState(IpcConnectionState.disconnected);
  }

  void _onLine(String line) {
    if (line.trim().isEmpty) return;
    try {
      final decoded = jsonDecode(line);
      if (decoded is! Map<String, dynamic>) return;

      final id = decoded['id'];
      if (id is int && id != 0) {
        _pending.remove(id)?.complete(decoded);
      } else if (decoded.containsKey('event') && !_events.isClosed) {
        _events.add(decoded);
      }
    } catch (e, stack) {
      debugPrint('JSON Parse Error on line: $line\n$e\n$stack');
    }
  }

  Future<Map<String, dynamic>> _sendRawRequest(Socket s, String action, dynamic payload) async {
    if (_pending.length >= _maxPendingRequests) {
      throw StateError('Max pending requests reached. Possible memory leak or daemon freeze.');
    }

    final id = _seq++;
    final completer = Completer<Map<String, dynamic>>();
    _pending[id] = completer;

    s.write('${jsonEncode({'id': id, 'action': action, if (payload != null) 'payload': payload})}\n');

    return completer.future.timeout(
      const Duration(seconds: 15),
      onTimeout: () {
        _pending.remove(id);
        throw TimeoutException('Request timed out: $action');
      },
    );
  }

  Future<Map<String, dynamic>> _call(String action, dynamic payload) async {
    if (_state != IpcConnectionState.connected) await connect();

    final s = _socket;
    if (s == null || _state != IpcConnectionState.connected) {
      throw StateError('Cannot send request: Client is not connected');
    }
    return _sendRawRequest(s, action, payload);
  }

  // ───────── Public API ─────────

  Future<ToolResult<Map<String, dynamic>>> getCapabilities() async {
    try {
      final res = await _call('get_capabilities', null);
      return res.getStr('status') == 'ok'
          ? ToolResult.success('Capabilities received', res.getMap('data'))
          : ToolResult.failure(res.getMap('error')?.getStr('message', 'Unknown error') ?? 'Unknown error');
    } catch (e) { return ToolResult.failure('Communication error', e); }
  }

  Future<ToolResult<List<dynamic>>> getDiscoveryPeers() async {
    try {
      final res = await _call('get_discovery_peers', null);
      return res.getStr('status') == 'ok' && res.getList('data') != null
          ? ToolResult.success('Peers retrieved', res.getList('data'))
          : ToolResult.success('No peers found', []);
    } catch (e) { return ToolResult.failure('Communication error', e); }
  }

  Future<ToolResult<List<DeviceInfo>>> listDevices() async {
    try {
      final res = await _call('list_devices', null);
      if (res.getStr('status') == 'ok' && res.getList('data') != null) {
        final parsed = res.getList('data')!.map((e) => DeviceInfo.fromJson(e is Map<String, dynamic> ? e : {})).toList();
        return ToolResult.success(parsed.isEmpty ? 'No devices found' : 'Devices retrieved', parsed);
      }
      return ToolResult.failure(res.getMap('error')?.getStr('message', 'Failed') ?? 'Failed');
    } catch (e) { return ToolResult.failure('Communication error', e); }
  }

  Future<ToolResult<String>> pairDevice(String id) async {
    if (!DeviceValidator.isValid(id)) return ToolResult.failure('Invalid device ID');
    try {
      final res = await _call('pair_device', {'device_id': id});
      return res.getStr('status') == 'ok'
          ? ToolResult.success(res.getStr('data', 'Pair requested'))
          : ToolResult.failure(res.getMap('error')?.getStr('message', 'Pair failed') ?? 'Pair failed');
    } catch (e) { return ToolResult.failure('Communication error', e); }
  }

  Future<ToolResult<String>> pingDevice(String id) async {
    if (!DeviceValidator.isValid(id)) return ToolResult.failure('Invalid device ID');
    try {
      final res = await _call('ping_device', {'device_id': id});
      return res.getStr('status') == 'ok'
          ? ToolResult.success(res.getStr('data', 'Ping OK'))
          : ToolResult.failure(res.getMap('error')?.getStr('message', 'Ping failed') ?? 'Ping failed');
    } catch (e) { return ToolResult.failure('Communication error', e); }
  }

  Future<ToolResult<String>> shareFile(String id, String path) async {
    if (!DeviceValidator.isValid(id)) return ToolResult.failure('Invalid device ID');
    if (path.trim().isEmpty) return ToolResult.failure('Path cannot be empty');
    try {
      final res = await _call('share_file', {'device_id': id, 'file_or_url': path});
      return res.getStr('status') == 'ok'
          ? ToolResult.success(res.getStr('data', 'File shared'))
          : ToolResult.failure(res.getMap('error')?.getStr('message', 'Share failed') ?? 'Share failed');
    } catch (e) { return ToolResult.failure('Communication error', e); }
  }

  Future<ToolResult<StreamSession>> startMirror({String? serial, bool camera = false, String? recording}) async {
    if (serial != null && serial.isNotEmpty && !DeviceValidator.isValid(serial)) {
      return ToolResult.failure('Invalid ADB serial');
    }
    try {
      final res = await _call('start_mirror', {'serial': serial?.isNotEmpty == true ? serial : null, 'camera': camera, 'record': recording});
      if (res.getStr('status') == 'ok' && res.getMap('data') != null) {
        final sess = StreamSession.fromJson(res.getMap('data')!);
        return ToolResult.success('Started scrcpy (PID: ${sess.pid ?? "unknown"})', sess);
      }
      return ToolResult.failure(res.getMap('error')?.getStr('message', 'Failed to start') ?? 'Failed to start');
    } catch (e) { return ToolResult.failure('Communication error', e); }
  }

  Future<ToolResult<String>> stopMirror({String? sessionId}) async {
    try {
      final res = await _call('stop_mirror', {'session_id': sessionId});
      return res.getStr('status') == 'ok'
          ? ToolResult.success('Stream stopped')
          : ToolResult.failure(res.getMap('error')?.getStr('message', 'Failed to stop') ?? 'Failed to stop');
    } catch (e) { return ToolResult.failure('Communication error', e); }
  }

  Future<ToolResult<StreamSession?>> getSession() async {
    try {
      final res = await _call('get_session', null);
      if (res.getStr('status') == 'ok' && res.getMap('data') != null) {
        return ToolResult.success('Session retrieved', StreamSession.fromJson(res.getMap('data')!));
      }
      return ToolResult.success('No active session', null);
    } catch (e) { return ToolResult.failure('Communication error', e); }
  }

  void dispose() {
    _cleanup(keepState: false);
    if (!_events.isClosed) _events.close();
    if (!_stateController.isClosed) _stateController.close();
  }
}
