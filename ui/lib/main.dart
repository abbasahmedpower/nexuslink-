import 'dart:async';
import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'backend.dart';

void main() => runApp(const NexusApp());

class NexusApp extends StatelessWidget {
  const NexusApp({super.key});
  @override
  Widget build(BuildContext context) => MaterialApp(
        debugShowCheckedModeBanner: false,
        title: 'Nexus Hub',
        theme: ThemeData(useMaterial3: true, colorSchemeSeed: const Color(0xff4263eb)),
        darkTheme: ThemeData(
            useMaterial3: true,
            colorSchemeSeed: const Color(0xff4263eb),
            brightness: Brightness.dark),
        themeMode: ThemeMode.system,
        home: const Dashboard(),
      );
}

class Dashboard extends StatefulWidget {
  const Dashboard({super.key});
  @override
  State<Dashboard> createState() => _DashboardState();
}

class _DashboardState extends State<Dashboard> {
  final ipc = NexusIpcClient();
  final deviceId = TextEditingController();
  final adbSerial = TextEditingController();
  final filePath = TextEditingController();
  int tab = 0;
  bool busy = false;
  String message = 'Nexus Hub v0.4: Connecting to secure Rust daemon...';
  String available = 'Press Refresh to list paired, reachable KDE Connect devices.';
  List<dynamic> discoveries = [];
  Timer? timer;
  StreamSubscription? eventSub;
  StreamSession? activeSession;

  static const tabs = ['Devices', 'Pairing', 'Mirroring', 'Camera', 'Files', 'About'];
  static const icons = [
    Icons.devices,
    Icons.link,
    Icons.screen_share,
    Icons.camera_alt,
    Icons.folder,
    Icons.info_outline
  ];

  @override
  void initState() {
    super.initState();
    refreshDiscovery();
    timer = Timer.periodic(const Duration(seconds: 8), (_) => refreshDiscovery());

    // Listen to streaming events from the Rust supervisor
    eventSub = ipc.events.listen((event) {
      if (!mounted) return;
      final type = event['event'] as String?;
      final data = event['data'];
      setState(() {
        if (type == 'session_started') {
          activeSession = StreamSession.fromJson(data as Map<String, dynamic>);
          message = 'Event: Session ${activeSession?.sessionId} started (PID: ${activeSession?.pid})';
        } else if (type == 'session_exited') {
          activeSession = null;
          message = 'Event: Session ${data['session_id']} exited (code: ${data['exit_code']})';
        } else if (type == 'session_failed') {
          activeSession = null;
          message = 'Event: Session ${data['session_id']} failed: ${data['error']}';
        }
      });
    });
  }

  @override
  void dispose() {
    timer?.cancel();
    eventSub?.cancel();
    deviceId.dispose();
    adbSerial.dispose();
    filePath.dispose();
    ipc.dispose();
    super.dispose();
  }

  Future<void> execute(Future<ToolResult> Function() action) async {
    if (busy) return;
    setState(() => busy = true);
    final result = await action();
    if (!mounted) return;
    setState(() {
      busy = false;
      message = '${result.ok ? "OK" : "ERROR"}: ${result.text}';
    });
  }

  Future<void> refreshKde() async {
    await execute(() async {
      final result = await ipc.listDevices();
      if (mounted) setState(() => available = result.text);
      return result;
    });
  }

  Future<void> refreshDiscovery() async {
    try {
      final response = await http
          .get(Uri.parse('http://127.0.0.1:8787/status'))
          .timeout(const Duration(seconds: 2));
      if (response.statusCode == 200 && mounted) {
        final data = jsonDecode(response.body) as Map<String, dynamic>;
        setState(() => discoveries = data['peers'] as List<dynamic>? ?? []);
      }
    } catch (_) {
      if (mounted) setState(() => discoveries = []);
    }
  }

  @override
  Widget build(BuildContext context) {
    final cs = Theme.of(context).colorScheme;
    return Scaffold(
        body: SafeArea(
            child: Row(children: [
      NavigationRail(
          extended: MediaQuery.sizeOf(context).width > 1020,
          selectedIndex: tab,
          onDestinationSelected: (i) => setState(() => tab = i),
          leading: const Padding(
              padding: EdgeInsets.all(18), child: Icon(Icons.hub_rounded, size: 30)),
          destinations: [
            for (var i = 0; i < tabs.length; i++)
              NavigationRailDestination(icon: Icon(icons[i]), label: Text(tabs[i]))
          ]),
      const VerticalDivider(width: 1),
      Expanded(
          child: Column(children: [
        Container(
            color: cs.surfaceContainerLow,
            padding: const EdgeInsets.all(18),
            child: Row(children: [
              const Text('Nexus Hub',
                  style: TextStyle(fontWeight: FontWeight.bold, fontSize: 22)),
              const Spacer(),
              Flexible(
                  child: Text('Rust Process Supervisor · Secure IPC',
                      style: TextStyle(color: cs.onSurfaceVariant)))
            ])),
        Expanded(
            child: SingleChildScrollView(
                padding: const EdgeInsets.all(24),
                child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
                  Text(tabs[tab],
                      style: const TextStyle(fontSize: 26, fontWeight: FontWeight.bold)),
                  const SizedBox(height: 18),
                  ...page(),
                  const SizedBox(height: 22),
                  Card(
                      child: Padding(
                          padding: const EdgeInsets.all(16),
                          child: SelectableText(message))),
                ]))),
      ])),
    ])));
  }

  Widget action(String label, Future<ToolResult> Function() onTap, IconData icon) =>
      Padding(
          padding: const EdgeInsets.only(right: 10, top: 10),
          child: FilledButton.icon(
              onPressed: busy ? null : () => execute(onTap),
              icon: Icon(icon),
              label: Text(label)));

  Widget field(TextEditingController c, String label) => Padding(
      padding: const EdgeInsets.only(bottom: 12),
      child: TextField(
          controller: c,
          decoration: InputDecoration(labelText: label, border: const OutlineInputBorder())));

  List<Widget> page() {
    switch (tab) {
      case 0:
        return [
          const Text(
              'KDE Connect manages trusted pairing via the Rust daemon adapter. Nexus discovery is an untrusted LAN view.'),
          action('Refresh KDE devices', () async {
            final r = await ipc.listDevices();
            if (mounted) setState(() => available = r.text);
            return r;
          }, Icons.refresh),
          Card(child: Padding(padding: const EdgeInsets.all(16), child: SelectableText(available))),
          const Text('Untrusted LAN discovery from Rust Core:'),
          for (final d in discoveries)
            ListTile(
                leading: const Icon(Icons.shield_outlined),
                title: Text('${d['name']}'),
                subtitle: Text('${d['address']} (unverified)'))
        ];
      case 1:
        return [
          field(deviceId, 'KDE device ID (from device list)'),
          Wrap(children: [
            action('Request pair', () => ipc.pairDevice(deviceId.text.trim()), Icons.link),
            action('Ping', () => ipc.pingDevice(deviceId.text.trim()), Icons.wifi_tethering)
          ]),
          const Text(
              'Accept the pairing request on both devices. Never treat an untrusted discovery result as paired.')
        ];
      case 2:
        return [
          field(adbSerial, 'ADB serial (optional)'),
          Wrap(children: [
            action('Start screen', () => ipc.startMirror(serial: adbSerial.text.trim()),
                Icons.play_arrow),
            action('Stop', () => ipc.stopMirror(), Icons.stop)
          ]),
          const Text(
              'Rust process supervisor launches scrcpy directly with typed arguments and monitors process exit/logs.')
        ];
      case 3:
        return [
          field(adbSerial, 'ADB serial (optional)'),
          Wrap(children: [
            action(
                'Start camera',
                () => ipc.startMirror(serial: adbSerial.text.trim(), camera: true),
                Icons.videocam),
            action('Stop', () => ipc.stopMirror(), Icons.stop)
          ]),
          const Text(
              'Camera capture requires supported Android/scrcpy. Managed as a supervised child process by Rust core.')
        ];
      case 4:
        return [
          field(deviceId, 'Paired KDE device ID'),
          field(filePath, 'Absolute local file path or URL'),
          action('Share via KDE Connect',
              () => ipc.shareFile(deviceId.text.trim(), filePath.text.trim()), Icons.send),
          const Text('Only send files to devices you own or trust.')
        ];
      default:
        return [
          const SelectableText(
              'Nexus Hub v0.4 — Secure IPC and managed child processes.\n'
              '• Rust owns subprocess lifecycle: scrcpy and kdeconnect-cli execution.\n'
              '• Flutter UI communicates exclusively via typed, per-user authenticated local IPC.\n'
              '• No untrusted shell interpolation; strict argument validation.\n'
              '• Calls, SMS, and in-app embedded video remain planned for future iterations.')
        ];
    }
  }
}
