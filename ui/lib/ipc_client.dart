import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

/// Single source of truth for the IPC endpoint descriptor, mirroring
/// `nexus_ipc::runtime_dir()` / `token_path()` / `endpoint_path()` in Rust.
/// If these paths drift from the Rust implementation, the handshake fails —
/// so keep BOTH sides in sync (contract covered by docs/IPC_CONTRACT.md).
class IpcPaths {
  static Directory runtimeDir() {
    if (Platform.isWindows) {
      final base = Platform.environment['LOCALAPPDATA'] ?? Directory.systemTemp.path;
      return Directory('$base\\nexus-hub');
    }
    final base = Platform.environment['XDG_RUNTIME_DIR'] ?? '/tmp';
    return Directory('$base/nexus-hub');
  }

  static File tokenFile() => File('${runtimeDir().path}/ipc-token');
  static File endpointFile() => File('${runtimeDir().path}/ipc-endpoint');
}

/// Length-prefixed JSON frame codec identical to the Rust side:
/// 4-byte big-endian length + UTF-8 JSON body, hard bound 64 KiB enforced
/// BEFORE allocation on both directions.
const int maxFrame = 65536;

class IpcResponse {
  final bool ok;
  final Object? data;
  final String? error;
  const IpcResponse(this.ok, this.data, this.error);
}

/// Authenticated local control-plane client. All device/mirror/camera actions
/// go through the supervised Rust core — Flutter never spawns tools directly.
class NexusIpcClient {
  Socket? _socket;
  StreamSubscription<Uint8List>? _sub;
  final Map<int, Completer<IpcResponse>> _pending = {};
  int _seq = 0;
  BytesBuilder? _partial;

  bool get connected => _socket != null;

  Future<void> connect() async {
    final tokenFile = IpcPaths.tokenFile();
    final endpointFile = IpcPaths.endpointFile();
    if (!await tokenFile.exists() || !await endpointFile.exists()) {
      throw const SocketException('Nexus core is not running (no endpoint descriptor)');
    }
    final token = (await tokenFile.readAsString()).trim();
    final endpoint = (await endpointFile.readAsString()).trim();
    final parts = endpoint.split(':');
    if (parts.length != 2) throw const SocketException('Malformed endpoint descriptor');
    final port = int.tryParse(parts[1]) ?? 0;

    _socket = await Socket.connect(InternetAddress.loopbackIPv4, port,
        timeout: const Duration(seconds: 2));
    _partial = BytesBuilder(copy: true);
    _sub = _socket!.listen(_onBytes, onError: _failAll, onDone: _failAll);
    _authToken = token;
  }

  late String _authToken;

  void _onBytes(Uint8List chunk) {
    _partial!.add(chunk);
    // Bound total buffered bytes BEFORE trusting any announced length (nano-level:
    // a hostile server must not be able to grow our memory without limit).
    while (_partial!.length >= 4) {
      final bytes = _partial!.toBytes();
      if (bytes.length < 4) break;
      final len = ByteData.sublistView(bytes, 0, 4).getUint32(0);
      if (len == 0 || len > maxFrame) {
        _failAll(const SocketException('bad frame'));
        return;
      }
      if (bytes.length < 4 + len) {
        _partial = BytesBuilder(copy: true)..add(bytes);
        return;
      }
      final body = utf8.decode(bytes.sublist(4, 4 + len), allowMalformed: false);
      _partial = BytesBuilder(copy: true)..add(bytes.sublist(4 + len));
      try {
        final json = jsonDecode(body) as Map<String, dynamic>;
        final seq = (json['seq'] as num?)?.toInt() ?? -1;
        final completer = _pending.remove(seq);
        completer?.complete(IpcResponse(
          json['ok'] == true,
          json['data'],
          json['error'] as String?,
        ));
      } catch (_) {/* ignore malformed responses; deadline will fire */}
    }
  }

  void _failAll(Object err) {
    for (final c in _pending.values) {
      if (!c.isCompleted) c.completeError(err);
    }
    _pending.clear();
    _socket?.destroy();
    _socket = null;
  }

  Future<IpcResponse> call(String method, [Map<String, dynamic>? params]) async {
    final socket = _socket;
    if (socket == null) throw const SocketException('not connected');
    final seq = ++_seq;
    final envelope = jsonEncode({
      'token': _authToken,
      'seq': seq,
      'method': method,
      'params': params ?? {},
      'version': 1,
    });
    final body = utf8.encode(envelope);
    if (body.length > maxFrame) {
      throw const ArgumentError('request too large');
    }
    final header = Uint8List(4);
    ByteData.sublistView(header).setUint32(0, body.length);
    final completer = Completer<IpcResponse>();
    _pending[seq] = completer;
    socket.add(header);
    socket.add(body);
    return completer.future.timeout(const Duration(seconds: 10), onTimeout: () {
      _pending.remove(seq);
      throw TimeoutException('core did not answer "$method" within 10s');
    });
  }

  // ---- Typed surface mirroring nexus-domain Command contracts ----
  Future<IpcResponse> ping() => call('ping');
  Future<IpcResponse> devicesList() => call('devices_list');
  Future<IpcResponse> processesList() => call('processes_list');
  Future<IpcResponse> discoveryPeers() => call('devices_list');
  Future<IpcResponse> mirrorStart({String? serial}) =>
      call('mirror_start', {if (serial != null && serial.isNotEmpty) 'serial': serial});
  Future<IpcResponse> cameraStart({String? serial}) =>
      call('camera_start', {if (serial != null && serial.isNotEmpty) 'serial': serial});
  Future<IpcResponse> processStop(int handle) => call('process_stop', {'handle': handle});
  Future<IpcResponse> devicePair(String deviceId) => call('device_pair', {'device_id': deviceId});
  Future<IpcResponse> devicePing(String deviceId) => call('device_ping', {'device_id': deviceId});

  Future<void> close() async {
    await _sub?.cancel();
    _socket?.destroy();
    _socket = null;
    _pending.clear();
  }
}
