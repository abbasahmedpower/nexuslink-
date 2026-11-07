import 'package:flutter_test/flutter_test.dart';
import 'package:nexus_hub/backend.dart';

void main() {
  group('Device Validation', () {
    test('accepts valid, safe identifiers', () {
      expect(DeviceValidator.isValid('abc-123._:x'), isTrue);
    });

    test('rejects empty, malicious, or oversized identifiers', () {
      expect(DeviceValidator.isValid(''), isFalse);
      expect(DeviceValidator.isValid('--help'), isFalse); // Injection attempt
      expect(DeviceValidator.isValid('abc;echo injected'), isFalse);
      expect(DeviceValidator.isValid('x' * 129), isFalse); // > 128 chars
    });
  });

  group('Data Models', () {
    test('DeviceInfo deserializes safely with defaults', () {
      final dev = DeviceInfo.fromJson({
        'id': 'device-1',
        'name': 'Pixel 8',
        'origin': 'kde_connect',
        'paired': true,
        'reachable': true,
      });

      expect(dev.id, 'device-1');
      expect(dev.name, 'Pixel 8');
      expect(dev.paired, isTrue);
      expect(dev.address, isNull); // Nano-level: verifying safe null fallback
    });

    test('StreamSession handles partial data gracefully', () {
      final sess = StreamSession.fromJson({
        'session_id': 'scrcpy-1',
        'kind': 'screen',
        'state': 'running',
        'started_at_ms': 123456789,
        'pid': 9999,
        // exit_code and last_error intentionally omitted to test robustness
      });

      expect(sess.sessionId, 'scrcpy-1');
      expect(sess.state, 'running');
      expect(sess.pid, 9999);
      expect(sess.exitCode, isNull);
    });
  });

  group('NexusIpcClient', () {
    test('initializes in a clean, disconnected state', () {
      final client = NexusIpcClient();

      // Micro-level: Checking both the robust enum state and the convenience getter
      expect(client.state, IpcConnectionState.disconnected);
      expect(client.isConnected, isFalse);

      client.dispose();
    });
  });
}
