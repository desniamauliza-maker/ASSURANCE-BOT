const fs = require('fs');
let c = fs.readFileSync('bot-assurance.js', 'utf8');

// 1. /produktivitas_hari - Add gaul to closed count and display
c = c.replace(
  "const closed = t.reguler.closed.length + t.unspec.closed.length + t.sqm.closed.length + t.manual.closed.length;\n            const open = t.reguler.open.length + t.unspec.open.length + t.sqm.open.length;",
  "const closed = t.reguler.closed.length + t.unspec.closed.length + t.sqm.closed.length + t.manual.closed.length + t.gaul.closed.length;\n            const open = t.reguler.open.length + t.unspec.open.length + t.sqm.open.length;"
);

// Add GAUL line after MANUAL GGN in produktivitas_hari
c = c.replace(
  "response += `└─ 🛠 MANUAL GGN: ${t.manual.closed.length} closed\\n\\n`;",
  "response += `├─ 🛠 MANUAL GGN: ${t.manual.closed.length} closed\\n`;\n            response += `└─ 🔄 GAUL: ${t.gaul.closed.length} detected\\n\\n`;"
);

// 2. /detail_team - Add gaul to counts  
c = c.replace(
  "const closed = t.reguler.closed.length + t.unspec.closed.length + t.sqm.closed.length + t.manual.closed.length;\n        const open = t.reguler.open.length + t.unspec.open.length + t.sqm.open.length;",
  "const closed = t.reguler.closed.length + t.unspec.closed.length + t.sqm.closed.length + t.manual.closed.length + t.gaul.closed.length;\n        const open = t.reguler.open.length + t.unspec.open.length + t.sqm.open.length;"
);

// Add GAUL section after MANUAL GGN in detail_team
c = c.replace(
  "response += `\\n╚══════════════════════════════════════╝`;",
  "response += '\\n';\n\n        // GAUL\n        response += `🔄 <b>GAUL</b> (${t.gaul.closed.length} detected)\\n`;\n        if (t.gaul.closed.length > 0) {\n          t.gaul.closed.forEach(tk => { response += `  • ${tk.incident} ✅\\n`; });\n        } else {\n          response += `└─ <i>Tidak ada data</i>\\n`;\n        }\n\n        response += `\\n╚══════════════════════════════════════╝`;"
);

// 3. /ringkasan_produk - Add gaul to teamStats  
c = c.replace(
  "const mc = t.manual.closed.length;\n          const closed = rc + uc + sc + mc;",
  "const mc = t.manual.closed.length;\n          const gc = t.gaul.closed.length;\n          const closed = rc + uc + sc + mc + gc;"
);

// Add GAUL total display after MANUAL GGN in ringkasan
c = c.replace(
  "response += `🛠 <b>MANUAL GGN:</b> ${totalManual.closed} closed\\n\\n`;",
  "response += `🛠 <b>MANUAL GGN:</b> ${totalManual.closed} closed\\n`;\n        let totalGaul = 0;\n        for (const [_, t] of Object.entries(teams)) { totalGaul += t.gaul.closed.length; }\n        response += `🔄 <b>GAUL:</b> ${totalGaul} detected\\n\\n`;"
);

// 4. /rank_team - Add gaul to counts
c = c.replace(
  "const closed = t.reguler.closed.length + t.unspec.closed.length + t.sqm.closed.length + t.manual.closed.length;\n          const open = t.reguler.open.length + t.unspec.open.length + t.sqm.open.length;",
  "const closed = t.reguler.closed.length + t.unspec.closed.length + t.sqm.closed.length + t.manual.closed.length + t.gaul.closed.length;\n          const open = t.reguler.open.length + t.unspec.open.length + t.sqm.open.length;"
);

// 5. Sort in /produktivitas_hari - add gaul to sort calc  
c = c.replace(
  "const totalA = teams[a].reguler.closed.length + teams[a].unspec.closed.length + teams[a].sqm.closed.length + teams[a].manual.closed.length;",
  "const totalA = teams[a].reguler.closed.length + teams[a].unspec.closed.length + teams[a].sqm.closed.length + teams[a].manual.closed.length + (teams[a].gaul ? teams[a].gaul.closed.length : 0);"
);
c = c.replace(
  "const totalB = teams[b].reguler.closed.length + teams[b].unspec.closed.length + teams[b].sqm.closed.length + teams[b].manual.closed.length;",
  "const totalB = teams[b].reguler.closed.length + teams[b].unspec.closed.length + teams[b].sqm.closed.length + teams[b].manual.closed.length + (teams[b].gaul ? teams[b].gaul.closed.length : 0);"
);

fs.writeFileSync('bot-assurance.js', c, 'utf8');

// Verify
const { execSync } = require('child_process');
try {
  execSync('node --check bot-assurance.js', { stdio: 'pipe' });
  console.log('✅ All GAUL display updates applied! Syntax OK.');
} catch (e) {
  console.log('❌ Syntax error:', e.stderr.toString().substring(0, 300));
}
