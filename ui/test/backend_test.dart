import 'package:flutter_test/flutter_test.dart';
import '../lib/backend.dart';

void main() {
  test('device identifiers are strictly validated', () {
    expect(validDeviceId('abc-123._:x'), true);
    expect(validDeviceId(''), false);
    expect(validDeviceId('--help'), false);
    expect(validDeviceId('abc;echo injected'), false);
    expect(validDeviceId('x' * 129), false);
  });

  test('DeviceInfo and StreamSession deserialize properly', () {
    final dev = DeviceInfo.fromJson({
      'id': 'device-1',
      'name': 'Pixel 8',
      'origin': 'kde_connect',
      'paired': true,
      'reachable': true,
    });
    expect(dev.id, 'device-1');
    expect(dev.name, 'Pixel 8');
    expect(dev.paired, true);

    final sess = StreamSession.fromJson({
      'session_id': 'scrcpy-1',
      'kind': 'screen',
      'state': 'running',
      'started_at_ms': 123456789,
      'pid': 9999,
    });
    expect(sess.sessionId, 'scrcpy-1');
    expect(sess.state, 'running');
    expect(sess.pid, 9999);
  });

  test('NexusIpcClient initializes disconnected and unauthenticated', () {
    final client = NexusIpcClient();
    expect(client.isConnected, false);
    client.dispose();
  });
}
