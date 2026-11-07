import 'dart:async';
import 'package:flutter/material.dart';
import 'backend.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const NexusApp());
}

class NexusApp extends StatelessWidget {
  const NexusApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'Nexus Hub',
      theme: ThemeData(useMaterial3: true, colorSchemeSeed: const Color(0xff4263eb)),
      darkTheme: ThemeData(useMaterial3: true, colorSchemeSeed: const Color(0xff4263eb), brightness: Brightness.dark),
      themeMode: ThemeMode.system,
      home: const Dashboard(),
    );
  }
}

class _DashboardController {
  final NexusIpcClient ipc = NexusIpcClient();

  List<DeviceInfo> devices = [];
  List<Map<String, dynamic>> discoveries = [];
  StreamSession? activeSession;

  bool isBusy = false;
  String statusMessage = 'جاري الاتصال بخادم Rust الآمن...';

  Timer? _refreshTimer;
  StreamSubscription<Map<String, dynamic>>? _eventSub;

  Future<void> initialize() async {
    try {
      await ipc.connect();
      statusMessage = 'متصل بنجاح ✅';
      await Future.wait([refreshDevices(), refreshDiscoveries(), refreshSession()]);

      _refreshTimer = Timer.periodic(const Duration(seconds: 10), (_) => refreshDiscoveries());
      _eventSub = ipc.events.listen(_handleEvent);
    } catch (e) {
      statusMessage = 'فشل الاتصال: $e';
    }
  }

  void dispose() {
    _refreshTimer?.cancel();
    _eventSub?.cancel();
    ipc.dispose();
  }

  Future<void> refreshDevices() async {
    final result = await ipc.listDevices();
    if (result.success && result.data != null) {
      devices = result.data!;
      statusMessage = result.message;
    } else {
      statusMessage = result.message;
    }
  }

  Future<void> refreshDiscoveries() async {
    final result = await ipc.getDiscoveryPeers();
    if (result.success && result.data != null) {
      discoveries = result.data!.cast<Map<String, dynamic>>();
    }
  }

  Future<void> refreshSession() async {
    final result = await ipc.getSession();
    if (result.success && result.data != null && result.data!.state == 'running') {
      activeSession = result.data;
    } else {
      activeSession = null;
    }
  }

  void _handleEvent(Map<String, dynamic> event) {
    final type = event['event'] as String?;
    final data = event['data'];

    if (type == 'resync_required') {
      refreshDevices();
      refreshSession();
      return;
    }

    if (type == 'session_started' && data is Map<String, dynamic>) {
      activeSession = StreamSession.fromJson(data);
      statusMessage = 'تم بدء الجلسة: ${activeSession?.sessionId} (PID: ${activeSession?.pid})';
    } else if (type == 'session_exited' && data is Map<String, dynamic>) {
      activeSession = null;
      statusMessage = 'انتهت الجلسة: ${data['session_id']} (كود الخروج: ${data['exit_code']})';
    } else if (type == 'session_failed' && data is Map<String, dynamic>) {
      activeSession = null;
      statusMessage = 'فشلت الجلسة: ${data['session_id']} - ${data['error']}';
    }
  }

  // Micro-level: تقبل أي دالة ترجع Future<void> لتجنب تعارض الأنواع
  Future<void> executeAction(Future<void> Function() action) async {
    if (isBusy) return;
    isBusy = true;
    try {
      await action();
    } catch (e) {
      statusMessage = 'خطأ غير متوقع: $e';
    } finally {
      isBusy = false;
    }
  }
}

class Dashboard extends StatefulWidget {
  const Dashboard({super.key});

  @override
  State<Dashboard> createState() => _DashboardState();
}

class _DashboardState extends State<Dashboard> {
  final _controller = _DashboardController();
  int _currentIndex = 0;

  final _deviceIdCtrl = TextEditingController();
  final _adbSerialCtrl = TextEditingController();
  final _filePathCtrl = TextEditingController();

  static const _tabs = ['الأجهزة', 'الاقتران', 'عرض الشاشة', 'الكاميرا', 'ملفات', 'حول'];
  static const _icons = [Icons.devices, Icons.link, Icons.screen_share, Icons.camera_alt, Icons.folder, Icons.info_outline];

  @override
  void initState() {
    super.initState();
    _controller.initialize();
  }

  @override
  void dispose() {
    _controller.dispose();
    _deviceIdCtrl.dispose();
    _adbSerialCtrl.dispose();
    _filePathCtrl.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: SafeArea(
        child: Row(
          children: [
            NavigationRail(
              extended: MediaQuery.sizeOf(context).width > 1020,
              selectedIndex: _currentIndex,
              onDestinationSelected: (index) => setState(() => _currentIndex = index),
              leading: const Padding(padding: EdgeInsets.all(18), child: Icon(Icons.hub_rounded, size: 32, color: Colors.blue)),
              destinations: [
                for (var i = 0; i < _tabs.length; i++)
                  NavigationRailDestination(icon: Icon(_icons[i]), label: Text(_tabs[i])),
              ],
            ),
            const VerticalDivider(width: 1),
            Expanded(
              child: Column(
                children: [
                  _buildHeader(context),
                  Expanded(
                    child: SingleChildScrollView(
                      padding: const EdgeInsets.all(24),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Text(_tabs[_currentIndex], style: Theme.of(context).textTheme.headlineMedium),
                          const SizedBox(height: 18),
                          ..._buildCurrentPage(),
                          const SizedBox(height: 24),
                          _buildStatusCard(context),
                        ],
                      ),
                    ),
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildHeader(BuildContext context) {
    final cs = Theme.of(context).colorScheme;
    return Container(
      color: cs.surfaceContainerLow,
      padding: const EdgeInsets.symmetric(horizontal: 24, vertical: 16),
      child: Row(
        children: [
          const Text('Nexus Hub', style: TextStyle(fontWeight: FontWeight.bold, fontSize: 20)),
          const Spacer(),
          Text(
            _controller.activeSession != null
                ? '🔴 جاري البث (PID: ${_controller.activeSession!.pid})'
                : 'Rust Process Supervisor · Secure IPC',
            style: TextStyle(
              color: _controller.activeSession != null ? Colors.redAccent : cs.onSurfaceVariant,
              fontWeight: FontWeight.w500,
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildStatusCard(BuildContext context) {
    return Card(
      elevation: 0,
      color: Theme.of(context).colorScheme.surfaceContainerHighest,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Icon(
              _controller.statusMessage.contains('خطأ') || _controller.statusMessage.contains('فشل') ? Icons.error_outline : Icons.info_outline,
              size: 20,
              color: _controller.statusMessage.contains('خطأ') || _controller.statusMessage.contains('فشل') ? Colors.red : Colors.blue,
            ),
            const SizedBox(width: 12),
            Expanded(
              child: SelectableText(_controller.statusMessage, style: const TextStyle(fontFamily: 'monospace', fontSize: 13)),
            ),
            if (_controller.isBusy) const SizedBox(width: 12, child: CircularProgressIndicator(strokeWidth: 2)),
          ],
        ),
      ),
    );
  }

  List<Widget> _buildCurrentPage() {
    switch (_currentIndex) {
      case 0: return _buildDevicesTab();
      case 1: return _buildPairingTab();
      case 2: return _buildMirroringTab();
      case 3: return _buildCameraTab();
      case 4: return _buildFilesTab();
      default: return _buildAboutTab();
    }
  }

  List<Widget> _buildDevicesTab() {
    return [
      const Text('إدارة الأجهزة الموثوقة عبر محول Rust Daemon. عرض الشبكة المحلي هو لأغراض الاكتشاف فقط.'),
      const SizedBox(height: 16),
      _ActionButton(label: 'تحديث أجهزة KDE', icon: Icons.refresh, isBusy: _controller.isBusy, onTap: () => _controller.executeAction(_controller.refreshDevices)),
      const SizedBox(height: 16),
      if (_controller.devices.isEmpty)
        const Card(child: Padding(padding: EdgeInsets.all(16), child: Text('لا توجد أجهزة مقترنة حالياً.')))
      else
        ..._controller.devices.map((device) => Card(
          child: ListTile(
            leading: const Icon(Icons.devices),
            title: Text(device.name),
            subtitle: Text('${device.id} • ${device.reachable ? "متصل" : "غير متصل"}'),
            trailing: Icon(device.paired ? Icons.verified : Icons.verified_outlined, color: device.paired ? Colors.green : Colors.grey),
          ),
        )),
      const SizedBox(height: 24),
      const Text('أجهزة الشبكة المحلي (غير موثقة):', style: TextStyle(fontWeight: FontWeight.bold)),
      const SizedBox(height: 8),
      if (_controller.discoveries.isEmpty)
        const Text('لم يتم العثور على أجهزة.', style: TextStyle(color: Colors.grey))
      else
        ..._controller.discoveries.map((d) => ListTile(
          leading: const Icon(Icons.shield_outlined, color: Colors.orange),
          title: Text(d['name']?.toString() ?? 'Unknown'),
          subtitle: Text('${d['address'] ?? 'No Address'} (غير موثق)'),
        )),
    ];
  }

  List<Widget> _buildPairingTab() {
    return [
      _InputField(controller: _deviceIdCtrl, label: 'معرف جهاز KDE (من قائمة الأجهزة)'),
      const SizedBox(height: 16),
      Wrap(spacing: 12, runSpacing: 12, children: [
        _ActionButton(
          label: 'طلب اقتران', icon: Icons.link, isBusy: _controller.isBusy,
          onTap: () => _controller.executeAction(() async {
            final result = await _controller.ipc.pairDevice(_deviceIdCtrl.text.trim());
            _controller.statusMessage = result.message;
          }),
        ),
        _ActionButton(
          label: 'اختبار الاتصال (Ping)', icon: Icons.wifi_tethering, isBusy: _controller.isBusy,
          onTap: () => _controller.executeAction(() async {
            final result = await _controller.ipc.pingDevice(_deviceIdCtrl.text.trim());
            _controller.statusMessage = result.message;
          }),
        ),
      ]),
      const SizedBox(height: 16),
      const Text('⚠️ يجب قبول طلب الاقتران على كلا الجهازين.', style: TextStyle(color: Colors.orange, fontSize: 13)),
    ];
  }

  List<Widget> _buildMirroringTab() {
    return [
      _InputField(controller: _adbSerialCtrl, label: 'معرف ADB (اختياري)'),
      const SizedBox(height: 16),
      Wrap(spacing: 12, runSpacing: 12, children: [
        _ActionButton(
          label: 'بدء عرض الشاشة', icon: Icons.play_arrow, isBusy: _controller.isBusy,
          onTap: () => _controller.executeAction(() async {
            final result = await _controller.ipc.startMirror(serial: _adbSerialCtrl.text.trim());
            _controller.statusMessage = result.message;
          }),
        ),
        _ActionButton(
          label: 'إيقاف', icon: Icons.stop, isBusy: _controller.isBusy,
          onTap: () => _controller.executeAction(() async {
            final result = await _controller.ipc.stopMirror();
            _controller.statusMessage = result.message;
          }),
        ),
      ]),
    ];
  }

  List<Widget> _buildCameraTab() {
    return [
      _InputField(controller: _adbSerialCtrl, label: 'معرف ADB (اختياري)'),
      const SizedBox(height: 16),
      Wrap(spacing: 12, runSpacing: 12, children: [
        _ActionButton(
          label: 'بدء الكاميرا', icon: Icons.videocam, isBusy: _controller.isBusy,
          onTap: () => _controller.executeAction(() async {
            final result = await _controller.ipc.startMirror(serial: _adbSerialCtrl.text.trim(), camera: true);
            _controller.statusMessage = result.message;
          }),
        ),
        _ActionButton(
          label: 'إيقاف', icon: Icons.stop, isBusy: _controller.isBusy,
          onTap: () => _controller.executeAction(() async {
            final result = await _controller.ipc.stopMirror();
            _controller.statusMessage = result.message;
          }),
        ),
      ]),
    ];
  }

  List<Widget> _buildFilesTab() {
    return [
      _InputField(controller: _deviceIdCtrl, label: 'معرف جهاز KDE المقترن'),
      const SizedBox(height: 12),
      _InputField(controller: _filePathCtrl, label: 'مسار الملف المحلي المطلق أو رابط URL'),
      const SizedBox(height: 16),
      _ActionButton(
        label: 'مشاركة عبر KDE Connect', icon: Icons.send, isBusy: _controller.isBusy,
        onTap: () => _controller.executeAction(() async {
          final result = await _controller.ipc.shareFile(_deviceIdCtrl.text.trim(), _filePathCtrl.text.trim());
          _controller.statusMessage = result.message;
        }),
      ),
    ];
  }

  List<Widget> _buildAboutTab() {
    return const [
      Card(
        child: Padding(
          padding: EdgeInsets.all(20),
          child: SelectableText(
            'Nexus Hub v0.4 — Secure IPC and managed child processes.\n\n'
            '• Rust owns subprocess lifecycle.\n'
            '• Flutter UI communicates via typed, authenticated local IPC.\n'
            '• No untrusted shell interpolation; strict argument validation.',
            style: TextStyle(fontFamily: 'monospace', height: 1.6),
          ),
        ),
      ),
    ];
  }
}

class _ActionButton extends StatelessWidget {
  final String label;
  final IconData icon;
  final bool isBusy;
  final VoidCallback onTap;
  const _ActionButton({required this.label, required this.icon, required this.isBusy, required this.onTap});

  @override
  Widget build(BuildContext context) {
    return FilledButton.icon(onPressed: isBusy ? null : onTap, icon: Icon(icon, size: 18), label: Text(label));
  }
}

class _InputField extends StatelessWidget {
  final TextEditingController controller;
  final String label;
  const _InputField({required this.controller, required this.label});

  @override
  Widget build(BuildContext context) {
    return TextField(controller: controller, decoration: InputDecoration(labelText: label, border: const OutlineInputBorder(), contentPadding: const EdgeInsets.symmetric(horizontal: 12, vertical: 16)));
  }
}
