import 'dart:async';
import 'dart:io';

/// Runs official binaries without shell interpolation. Never treats process output as trusted.
class ToolResult {
  final bool ok;
  final String text;
  const ToolResult(this.ok, this.text);
}

class NexusTools {
  NexusTools({this.kdeExecutable = 'kdeconnect-cli', this.scrcpyExecutable = 'scrcpy'});
  final String kdeExecutable;
  final String scrcpyExecutable;
  Process? _mirror;
  bool get mirroring => _mirror != null;

  Future<ToolResult> _run(String executable, List<String> args) async {
    try {
      final result = await Process.run(executable, args, runInShell: false)
          .timeout(const Duration(seconds: 12));
      final stdout = result.stdout.toString().trim();
      final stderr = result.stderr.toString().trim();
      final output = [stdout, stderr].where((v) => v.isNotEmpty).join('\n');
      return ToolResult(result.exitCode == 0, output.isEmpty ? 'Exit: ${result.exitCode}' : output.substring(0, output.length > 12000 ? 12000 : output.length));
    } on TimeoutException {
      return const ToolResult(false, 'Tool timed out. Check the KDE Connect daemon.');
    } on ProcessException catch (e) {
      return ToolResult(false, 'Tool unavailable: ${e.message}');
    } catch (e) {
      return ToolResult(false, 'Execution error: $e');
    }
  }

  static bool validDeviceId(String id) =>
      RegExp(r'^[A-Za-z0-9_.:-]{1,128}$').hasMatch(id);

  Future<ToolResult> devices() => _run(kdeExecutable, ['--list-available', '--id-name-only']);
  Future<ToolResult> requestPair(String id) => validDeviceId(id)
      ? _run(kdeExecutable, ['--pair', '--device', id])
      : Future.value(const ToolResult(false, 'Invalid device id'));
  Future<ToolResult> ping(String id) => validDeviceId(id)
      ? _run(kdeExecutable, ['--ping', '--device', id])
      : Future.value(const ToolResult(false, 'Invalid device id'));
  Future<ToolResult> share(String id, String path) => validDeviceId(id) && path.trim().isNotEmpty
      ? _run(kdeExecutable, ['--share', path, '--device', id])
      : Future.value(const ToolResult(false, 'Invalid device id or path'));

  Future<ToolResult> startMirror({String? serial, bool camera = false, String? recording}) async {
    if (_mirror != null) return const ToolResult(false, 'A stream is already running.');
    if (serial != null && serial.isNotEmpty && !validDeviceId(serial)) {
      return const ToolResult(false, 'Invalid ADB serial');
    }
    final args = <String>[
      if (serial != null && serial.isNotEmpty) ...['--serial', serial],
      if (camera) '--video-source=camera',
      if (recording != null) ...['--record', recording],
      '--max-fps=60',
    ];
    try {
      final proc = await Process.start(scrcpyExecutable, args, runInShell: false);
      _mirror = proc;
      // Drain pipes to avoid blocking a long-running process.
      proc.stdout.listen((_) {});
      proc.stderr.listen((_) {});
      unawaited(proc.exitCode.then((_) { if (identical(_mirror, proc)) _mirror = null; }));
      return const ToolResult(true, 'Started scrcpy. Its video opens in a separate native window.');
    } on ProcessException catch (e) {
      return ToolResult(false, 'scrcpy unavailable: ${e.message}');
    }
  }

  Future<ToolResult> stopMirror() async {
    final proc = _mirror;
    if (proc == null) return const ToolResult(false, 'No active stream owned by Nexus.');
    proc.kill();
    try {
      await proc.exitCode.timeout(const Duration(seconds: 3));
    } on TimeoutException {
      return const ToolResult(false, 'Stop requested but termination not confirmed.');
    }
    if (identical(_mirror, proc)) _mirror = null;
    return const ToolResult(true, 'Stream stopped');
  }
}
