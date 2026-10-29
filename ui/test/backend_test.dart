import 'package:flutter_test/flutter_test.dart';
import '../lib/backend.dart';

void main() {
  test('device identifiers are strictly validated', () {
    expect(NexusTools.validDeviceId('abc-123._:x'), true);
    expect(NexusTools.validDeviceId(''), false);
    expect(NexusTools.validDeviceId('--help'), false);
    expect(NexusTools.validDeviceId('abc;echo injected'), false);
    expect(NexusTools.validDeviceId('x' * 129), false);
  });
}
