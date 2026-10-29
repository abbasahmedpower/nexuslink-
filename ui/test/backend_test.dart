import 'package:flutter_test/flutter_test.dart';
import '../lib/backend.dart';

void main() {
  group('backend device identifier validation', () {
    test('strictly validates allowed identifiers', () {
      expect(validDeviceId('abc-123._:x'), true);
      expect(validDeviceId('192.168.1.100:5555'), true);
      expect(validDeviceId(''), false);
      expect(validDeviceId('--help'), false);
      expect(validDeviceId('-s'), false);
      expect(validDeviceId('abc;echo injected'), false);
      expect(validDeviceId('device with space'), false);
      expect(validDeviceId('x' * 129), false);
    });
  });

  group('model serialization tests', () {
    test('parses DeviceInfo from JSON correctly', () {
      final json = {
        'id': 'device-01',
        'name': 'Pixel 8',
        'origin': 'kde_connect',
        'paired': true,
        'reachable': true,
        'address': '192.168.1.50',
      };
      final dev = DeviceInfo.fromJson(json);
      expect(dev.id, 'device-01');
      expect(dev.name, 'Pixel 8');
      expect(dev.paired, true);
      expect(dev.address, '192.168.1.50');
    });

    test('parses StreamSession from JSON correctly', () {
      final json = {
        'session_id': 'scrcpy-1',
        'kind': 'screen',
        'device_serial': 'emulator-5554',
        'state': 'running',
        'started_at_ms': 1700000000000,
        'pid': 1234,
        'exit_code': null,
        'last_error': null,
      };
      final session = StreamSession.fromJson(json);
      expect(session.sessionId, 'scrcpy-1');
      expect(session.kind, 'screen');
      expect(session.pid, 1234);
      expect(session.deviceSerial, 'emulator-5554');
      expect(session.state, 'running');
    });
  });
}
