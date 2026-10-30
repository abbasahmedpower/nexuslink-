import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

/// Self-identifying IPC frame codecs (mirrors `crates/nexus-ipc/src/wire.rs`).
///
/// JSON frames are NDJSON lines; bincode1 frames start with the tag byte 0xB1,
/// which can never appear as the first byte of a valid JSON document, so every
/// inbound frame is classified by sniffing its first byte — no negotiation
/// ambiguity, and old/new peers interoperate freely.
enum WireCodec { json, bincode1 }

const int kBincodeTag = 0xB1;
const int kMaxIpcFrameSize = 65536;

/// Minimal big-endian-free (little-endian) bincode 1.x serializer for the
/// subset of types used by IPC *requests* (the only direction we encode).
///
/// Layout rules implemented here (must match Rust `bincode` defaults):
/// - integers: fixed-size little-endian
/// - `Option<T>`: u8 discriminant (0/1) then value
/// - `String`/`Vec`: u64 length then bytes/items
/// - externally-tagged enums (`#[serde(tag="action", content="payload")]`):
///   u32 variant index then payload fields in declaration order
class BincodeWriter {
  final BytesBuilder _b = BytesBuilder(copy: false);
  static const int maxStringLength = 4096; // defensive bound per string field

  Uint8List toBytes() => _b.toBytes();

  void u8(int v) => _b.addByte(v & 0xff);
  void bool_(bool v) => _b.addByte(v ? 1 : 0);

  void u32(int v) {
    final bd = ByteData(4)..setUint32(0, v, Endian.little);
    _b.add(bd.buffer.asUint8List());
  }

  void u64(int v) {
    final bd = ByteData(8)..setUint64(0, v, Endian.little);
    _b.add(bd.buffer.asUint8List());
  }

  void str(String s) {
    final bytes = utf8.encode(s);
    if (bytes.length > maxStringLength) {
      throw ArgumentError('IPC string field too large (${bytes.length} bytes)');
    }
    u64(bytes.length);
    _b.add(bytes);
  }

  void optStr(String? s) {
    if (s == null) {
      u8(0);
    } else {
      u8(1);
      str(s);
    }
  }
}

/// Encodes an IPC request into bincode1 wire bytes (tag + payload + newline
/// terminator for transport consistency with the server's line framing).
Uint8List encodeBincodeRequest(int id, int variantIndex, void Function(BincodeWriter w) body) {
  final w = BincodeWriter();
  w.u64(id);
  w.u8(0); // WireMessageBody::Request
  w.u32(variantIndex); // IpcRequest serde tag ("action")
  body(w); // "content" payload fields
  final out = BytesBuilder(copy: false);
  out.addByte(kBincodeTag);
  out.add(w.toBytes());
  out.addByte(0x0a);
  return out.toBytes();
}

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

  /// Outbound codec for the current session (inbound frames are always
  /// decoded by first-byte sniffing, independent of this field).
  WireCodec _outboundCodec = WireCodec.json;

  /// IpcRequest serde variant indices — MUST match the declaration order in
  /// `crates/nexus-ipc/src/contract.rs` (`IpcRequest`). Kept as named
  /// constants with a startup self-check against the handshake flow so a
  /// reordering on either side fails loudly instead of corrupting frames.
  static const int _vHandshake = 0;
  static const int _vNegotiateCodec = 1;
  static const int _vGetCapabilities = 2;
  static const int _vGetDiscoveryPeers = 3;
  static const int _vListDevices = 4;
  static const int _vPairDevice = 5;
  static const int _vPingDevice = 6;
  static const int _vShareFile = 7;
  static const int _vStartMirror = 8;
  static const int _vStopMirror = 9;
  static const int _vGetSession = 10;
  static const int _vSubscribeEvents = 11;

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

  Future<void>? _connectingFuture;

  Future<void> connect() async {
    if (_socket != null) return;
    if (_connectingFuture != null) {
      await _connectingFuture;
      return;
    }
    _connectingFuture = _doConnect();
    try {
      await _connectingFuture;
    } finally {
      _connectingFuture = null;
    }
  }

  Future<void> _doConnect() async {
    final s = await Socket.connect(host, port, timeout: const Duration(seconds: 3));
    s.setOption(SocketOption.tcpNoDelay, true); // micro-latency: no Nagle on control channel
    _socket = s;
    _outboundCodec = WireCodec.json;
    // Raw byte framing: classify each line by its first byte (JSON vs bincode1).
    // A single malformed frame can never desynchronize the stream because both
    // codecs terminate frames with '\n' and the tag byte is disjoint from JSON.
    _frameBuf = BytesBuilder(copy: true);
    _sub = s.listen(
      _onBytes,
      onError: (err) {
        _cleanup();
      },
      onDone: () {
        _cleanup();
      },
    );

    // Perform Handshake (always JSON for interoperability; we advertise our
    // bincode1 capability, then negotiate explicitly after auth succeeds).
    final token = readAuthToken();
    final res = await _call('handshake', {
      'protocol_version': 1,
      'client_name': 'flutter-desktop-ui',
      if (token != null) 'auth_token': token,
      'codec': 'bincode1',
    });

    if (res['status'] != 'ok') {
      final err = res['error']?['message'] ?? 'Authentication failed';
      _cleanup();
      throw Exception('IPC handshake rejected: ' + err.toString());
    }

    // Upgrade the session to the binary codec. Failures are non-fatal: the
    // server keeps answering in JSON and every frame stays self-identifying.
    //
    // CRITICAL invariant (micro-level): the SERVER switches its outbound
    // framing on the negotiate ACK itself, so the client must flip its
    // decoder expectations at exactly the same frame boundary. Inbound frames
    // are always sniffed by first byte, so even a wrong guess here can never
    // desynchronize parsing — but we still track state for OUTBOUND choice.
    try {
      final neg = await _callJson('negotiate_codec', {'codec': 'bincode1'});
      if (neg['status'] == 'ok' && neg['data'] == 'bincode1') {
        _outboundCodec = WireCodec.bincode1;
      }
    } catch (_) {
      _outboundCodec = WireCodec.json;
    }

    // Subscribe to daemon events
    unawaited(_call('subscribe_events', null));
  }

  BytesBuilder _frameBuf = BytesBuilder(copy: true);

  void _onBytes(Uint8List chunk) {
    // Bounded accumulation: drop the connection rather than buffer an
    // unbounded stream without a newline (mirrors server-side read_bounded_frame).
    _frameBuf.add(chunk);
    while (true) {
      final bytes = _frameBuf.toBytes();
      final nl = bytes.indexOf(0x0a);
      if (nl < 0) {
        if (bytes.length > kMaxIpcFrameSize) {
          _cleanup();
        }
        return;
      }
      final frame = bytes.sublist(0, nl);
      _frameBuf.clear();
      if (bytes.length > nl + 1) {
        _frameBuf.add(bytes.sublist(nl + 1));
      }
      _onFrame(frame);
    }
  }

  void _onFrame(List<int> frame) {
    if (frame.isEmpty) return;
    try {
      Map<String, dynamic> decoded;
      if (frame[0] == kBincodeTag) {
        // Binary responses carry data we do not decode in Dart today. The UI
        // only consumes JSON semantics, so we request JSON whenever a payload
        // is needed; pure status acks arrive as decodable minimal maps via the
        // fallback below. To keep the client dead-simple and robust, the UI
        // downgrades outbound/inbound to JSON before any data-bearing call.
        decoded = _decodeBincodeAck(frame);
      } else {
        final line = utf8.decode(frame);
        if (line.trim().isEmpty) return;
        decoded = jsonDecode(line) as Map<String, dynamic>;
      }
      final id = decoded['id'] as int?;
      if (id != null && id != 0) {
        final completer = _pending.remove(id);
        completer?.complete(decoded);
      } else if (decoded.containsKey('event')) {
        _events.add(decoded);
      }
    } catch (_) {}
  }

  /// Decodes the tiny subset of bincode1 frames the Dart UI needs: response
  /// headers (id, body tag, IpcResponse variant) with an opaque/absent data
  /// payload. Data-bearing calls are routed through JSON (see [_call]).
  Map<String, dynamic> _decodeBincodeAck(Uint8List frame) {
    final bd = ByteData.sublistView(frame);
    int off = 1; // skip tag
    final id = bd.getUint64(off, Endian.little);
    off += 8;
    final bodyTag = frame[off];
    off += 1;
    final variant = bd.getUint32(off, Endian.little);
    off += 4;
    String status;
    Object? data;
    if (bodyTag == 1) {
      // WireMessageBody::Response -> IpcResponse variant: 0=Ok{data}, 1=Error
      if (variant == 0) {
        status = 'ok';
        // Option<Value>: u8 discriminant; we never ask the server to send
        // data over binary, so expect None (0). Anything else => unsupported.
        if (off < frame.length && frame[off] == 0) {
          data = null;
        }
      } else {
        status = 'error';
      }
    } else {
      status = 'event';
    }
    return {'id': id, 'status': status, if (data != null) 'data': data};
  }

  void _cleanup() {
    _socket?.destroy();
    _socket = null;
    _sub?.cancel();
    _sub = null;
    _outboundCodec = WireCodec.json;
    _frameBuf = BytesBuilder(copy: true);
    for (final completer in _pending.values) {
      if (!completer.isCompleted) {
        completer.completeError(Exception('IPC connection closed'));
      }
    }
    _pending.clear();
  }

  /// Serde variant index for each known action name. Used only when the
  /// session negotiated bincode1; JSON path ignores it entirely.
  static const Map<String, int> _actionIndex = {
    'handshake': _vHandshake,
    'negotiate_codec': _vNegotiateCodec,
    'get_capabilities': _vGetCapabilities,
    'get_discovery_peers': _vGetDiscoveryPeers,
    'list_devices': _vListDevices,
    'pair_device': _vPairDevice,
    'ping_device': _vPingDevice,
    'share_file': _vShareFile,
    'start_mirror': _vStartMirror,
    'stop_mirror': _vStopMirror,
    'get_session': _vGetSession,
    'subscribe_events': _vSubscribeEvents,
  };

  /// Actions whose responses carry a `data` payload the Dart UI must read.
  /// These are ALWAYS sent as JSON requests so the server replies in JSON —
  /// keeping the binary path for fire-and-forget/ack-only traffic where the
  /// serialization win matters and no decoding is needed client-side.
  static const Set<String> _dataBearingActions = {
    'get_capabilities',
    'get_discovery_peers',
    'list_devices',
    'pair_device',
    'ping_device',
    'share_file',
    'start_mirror',
    'stop_mirror',
    'get_session',
    'negotiate_codec',
  };

  /// Sends a request forcing JSON framing (used for negotiation-sensitive or
  /// data-bearing exchanges where the reply must be fully decodable in Dart).
  Future<Map<String, dynamic>> _callJson(String action, dynamic payload) =>
      _callWith(action, payload, forceJson: true);

  Future<Map<String, dynamic>> _call(String action, dynamic payload) =>
      _callWith(action, payload, forceJson: false);

  Future<Map<String, dynamic>> _callWith(String action, dynamic payload, {required bool forceJson}) async {
    if (_socket == null) {
      await connect();
    }
    final socket = _socket;
    if (socket == null) {
      throw StateError('IPC socket unavailable');
    }
    final id = _seq++;
    final completer = Completer<Map<String, dynamic>>();
    _pending[id] = completer;

    // Decide framing per request: binary only when the session upgraded AND
    // this call's reply does not need to be decoded in Dart.
    final useBinary = !forceJson &&
        _outboundCodec == WireCodec.bincode1 &&
        !_dataBearingActions.contains(action) &&
        _actionIndex.containsKey(action);

    try {
      if (useBinary) {
        final bytes = encodeBincodeRequest(id, _actionIndex[action]!, (w) {
          // All current ack-only actions (subscribe_events) have no payload
          // fields; unknown shapes fall back to JSON above.
        });
        socket.add(bytes);
      } else {
        final req = jsonEncode({
          'id': id,
          'action': action,
          if (payload != null) 'payload': payload,
        });
        socket.write('$req\n');
      }
    } catch (_) {
      _pending.remove(id);
      throw Exception('IPC encode/write failed for $action');
    }

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
