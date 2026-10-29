import 'dart:async';
import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;

void main() => runApp(const NexusApp());
class NexusApp extends StatelessWidget {
  const NexusApp({super.key});
  @override
  Widget build(BuildContext context) => MaterialApp(
    debugShowCheckedModeBanner: false,
    title: 'Nexus Hub',
    theme: ThemeData(useMaterial3: true, colorSchemeSeed: const Color(0xff4263eb), brightness: Brightness.light),
    darkTheme: ThemeData(useMaterial3: true, colorSchemeSeed: const Color(0xff748ffc), brightness: Brightness.dark),
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
  int section = 0;
  List<dynamic> devices = [];
  String state = 'Checking local Rust daemon...';
  Timer? refreshTimer;
  static const names = ['Devices', 'Notifications', 'Messages', 'Photos', 'Mirroring', 'Camera', 'Settings'];
  static const icons = [Icons.devices, Icons.notifications_outlined, Icons.message_outlined,
    Icons.photo_outlined, Icons.screen_share_outlined, Icons.camera_alt_outlined, Icons.settings_outlined];
  @override
  void initState() { super.initState(); refresh(); refreshTimer = Timer.periodic(const Duration(seconds: 4), (_) => refresh()); }
  Future<void> refresh() async {
    try {
      final response = await http.get(Uri.parse('http://127.0.0.1:8787/status')).timeout(const Duration(seconds: 2));
      if (response.statusCode != 200) throw Exception('HTTP ${response.statusCode}');
      final data = jsonDecode(response.body) as Map<String, dynamic>;
      if (!mounted) return;
      setState(() { devices = data['peers'] as List<dynamic>? ?? []; state = 'Discovery active · pairing not implemented'; });
    } catch (_) { if (mounted) setState(() { devices = []; state = 'Offline · start nexus-core first'; }); }
  }
  @override
  void dispose() { refreshTimer?.cancel(); super.dispose(); }
  @override
  Widget build(BuildContext context) {
    final cs = Theme.of(context).colorScheme;
    return Scaffold(body: SafeArea(child: Row(children: [
      NavigationRail(
        extended: MediaQuery.sizeOf(context).width > 940,
        selectedIndex: section,
        onDestinationSelected: (i) => setState(() => section = i),
        leading: const Padding(padding: EdgeInsets.symmetric(vertical: 20), child: Icon(Icons.hub_rounded, size: 32)),
        destinations: [for (var i = 0; i < names.length; i++) NavigationRailDestination(icon: Icon(icons[i]), label: Text(names[i]))],
      ),
      const VerticalDivider(width: 1),
      Expanded(child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
        Container(padding: const EdgeInsets.all(22), color: cs.surfaceContainerLow, child: Row(children: [
          const Icon(Icons.hub_rounded), const SizedBox(width: 10),
          const Text('Nexus Hub', style: TextStyle(fontSize: 22, fontWeight: FontWeight.bold)),
          const Spacer(), Flexible(child: Text(state, textAlign: TextAlign.end, style: TextStyle(color: cs.onSurfaceVariant))),
        ])),
        Expanded(child: Padding(padding: const EdgeInsets.all(28), child: section == 0 ? devicesView() : featureView())),
      ])),
    ])));
  }
  Widget devicesView() => Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
    const Text('Nearby devices', style: TextStyle(fontSize: 26, fontWeight: FontWeight.bold)),
    const SizedBox(height: 8),
    const Text('Passive discovery only. Devices are NOT paired, authenticated, or controllable.'),
    const SizedBox(height: 24),
    OutlinedButton.icon(onPressed: refresh, icon: const Icon(Icons.refresh), label: const Text('Refresh')),
    const SizedBox(height: 20),
    if (devices.isEmpty) const Card(child: Padding(padding: EdgeInsets.all(25), child: Text('No discovered peers yet. Verify both devices are on the same LAN and firewall allows UDP 1716.'))),
    Expanded(child: ListView(children: [for (final item in devices)
      Card(child: ListTile(leading: const Icon(Icons.smartphone), title: Text('${item['name']}'),
        subtitle: Text('${item['address']} · Discovered (untrusted)'), trailing: const Icon(Icons.lock_outline)))
    ])),
  ]);
  Widget featureView() => Center(child: ConstrainedBox(constraints: const BoxConstraints(maxWidth: 520),
    child: Column(mainAxisSize: MainAxisSize.min, children: [
      Icon(icons[section], size: 56), const SizedBox(height: 20),
      Text(names[section], style: const TextStyle(fontSize: 28, fontWeight: FontWeight.bold)),
      const SizedBox(height: 12),
      const Text('Planned module — unavailable in this discovery-only starter. No simulated actions or fake connected state.', textAlign: TextAlign.center),
    ])));
}
