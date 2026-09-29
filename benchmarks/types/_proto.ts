import ts from "typescript";
const root = "/home/user/kysely-hydrate";
const cfg = ts.getParsedCommandLineOfConfigFile(root + "/tsconfig.json", {}, { ...ts.sys, onUnRecoverableConfigFileDiagnostic: (d) => { throw new Error(String(d.messageText)); } })!;
const file = process.argv[2]!;
let old: ts.Program | undefined;
for (let i = 0; i < 4; i++) {
  const p = ts.createProgram({ rootNames: [file], options: cfg.options, oldProgram: old });
  old = p;
  const c = p.getTypeChecker();
  const i0 = p.getInstantiationCount(), t0 = p.getTypeCount();
  const s = performance.now();
  const d = p.getSemanticDiagnostics(p.getSourceFile(file));
  const ms = performance.now() - s;
  console.log(i0, t0, p.getInstantiationCount() - i0, p.getTypeCount() - t0, ms.toFixed(1), d.length, d.map(x => ts.flattenDiagnosticMessageText(x.messageText, "\n")).slice(0,2));
}
