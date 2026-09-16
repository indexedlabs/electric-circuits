import { createHash } from 'node:crypto'
import { readFileSync, writeFileSync, existsSync } from 'node:fs'
import { resolve } from 'node:path'
import { execFileSync } from 'node:child_process'

type Json = Record<string, unknown>
type Profile = { lane: 'COMPAT_V1' | 'NATIVE_CORE'; features: string[] }
type Edge = { requires: string; type: 'integrated' } | { requires: string; type: 'red_artifact'; provider: string; consumer: string; scenario_registry_requirement: 'registered_current_base_reviewed'; scope: 'profile'; base: 'current_integration_base'; independent_review: true }
type Task = { id: string; title: string; dependencies: Edge[]; applicability: string; execution_scope: 'shared_producer' | 'per_profile'; proof_kind: 'non_behavioral' | 'genuine_red' | 'inherited_control'; owner: string; principal_write_boundary: string; artifacts: string[]; read_resources: string[]; write_resources: string[]; semantic_resources: string[]; runtime_resources: string[]; scenario_ids: string[]; acceptance: string }

const validatorRoot = resolve(import.meta.dirname, '..')
const root = resolve(process.cwd())
const paths = {
  tasks: resolve(root, 'docs/production/readiness-tasks.json'),
  schema: resolve(root, 'docs/production/readiness-task.schema.json'),
  gates: resolve(root, 'docs/production/readiness-gates.json'),
  report: resolve(root, 'docs/production/readiness-plan.generated.md'),
  validator: resolve(root, 'scripts/readiness-plan.ts'),
  validator_tests: resolve(root, 'scripts/readiness-plan.test.ts'),
}
const authorityPins = [
  { path: 'notes/18-production-readiness-spec-reviewed.md', blob: '5b02585cf7224563dc7246ba310e5621d8b98b67' },
  { path: 'notes/skills-research/05-parallel-agent-execution-protocol.md', blob: '29d84268de4a6cf5e12868ba5738a85c624f095c' },
] as const
const integrationCommit = '520751ef250abd4936720e1e7e0c620a158833a0'
const integrationTree = '8374caa150215bb0ad74c535769fc978e0e57af7'
const sha256 = (value: string) => createHash('sha256').update(value).digest('hex')
const canonical = (value: unknown) => JSON.stringify(value, (_key, entry) => entry && typeof entry === 'object' && !Array.isArray(entry) ? Object.fromEntries(Object.entries(entry).sort(([a], [b]) => a.localeCompare(b))) : entry)
const fail = (reason: string): never => { throw new Error(reason) }
const git = (...args: string[]) => execFileSync('git', args, { cwd: validatorRoot, encoding: 'utf8' }).trim()
const checkoutIdentity = () => ({ head: git('rev-parse', 'HEAD'), tree: git('rev-parse', 'HEAD^{tree}') })
let authoritiesValidated = false
export const validateAuthorities = () => {
  if (authoritiesValidated) return
  if (git('rev-parse', `${integrationCommit}^{tree}`) !== integrationTree) fail('authority_integration_tree_mismatch')
  for (const authority of authorityPins) { const file = resolve(validatorRoot, authority.path); if (git('hash-object', file) !== authority.blob || git('rev-parse', `HEAD:${authority.path}`) !== authority.blob) fail('authority_blob_mismatch') }
  authoritiesValidated = true
}
export const outputIdentities = (includeReport = true) => {
  const entries = Object.entries(paths).filter(([name]) => includeReport || name !== 'report').map(([name, path]) => {
    const bytes = readFileSync(path, 'utf8')
    return { name, path: path.slice(root.length + 1), git_blob_sha: git('hash-object', path), sha256: sha256(bytes) }
  })
  return { outputs: entries, bundle_sha256: sha256(canonical(entries)) }
}

// This compact inventory is generated from the two bootstrap authorities at PLAN-001 creation time.
// The checked-in JSON, not those prose inputs, is subsequently the scheduler authority.
const taskLines = `PLAN-001|
GOV-001|PLAN-001
GOV-002|GOV-001
CMP-000|GOV-001
GOV-003|GOV-001,PLAN-001
GOV-004|GOV-001,SEC-000,PROTO-001A,PROTO-001B
GOV-005|GOV-002,CMP-000,CMP-001,PLAN-001
NATIVE-ADR-001|CMP-001,GOV-002
RLS-001|GOV-004,SEC-008A,SEC-008B,GWR-002,PGR-001,TST-003
SEC-000|SEC-001,CMP-001
PROTO-001A|SEC-000
PROTO-001B|SEC-000
PROTO-001C|PROTO-001A,PROTO-001B,GOV-002,CMP-001
PROTO-001D|PROTO-001C,SEC-003
PROTO-002|PROTO-001A,PROTO-001B
PROTO-003A|PROTO-001A,PROTO-001B,PROTO-002
PROTO-003B|PROTO-003A,ENG-002
PROTO-004|PROTO-002,PROTO-003A
SEC-001|GOV-002
SEC-002A|SEC-000,E2E-002R
GWR-001|GOV-004,SEC-001,SEC-006A,CAP-001A,E2E-002R
SEC-002B|SEC-002A,PROTO-001A,PROTO-002,GWR-001,E2E-002R
GWR-002|GWR-001,SEC-002B,E2E-002R
SEC-002C|SEC-002B,SEC-003,E2E-002R
SEC-003|SEC-000,CMP-001,E2E-002R
SEC-003B|SEC-003,ENG-017
SEC-004|SEC-002B,SEC-003,PROTO-003A,E2E-002R
SEC-005A|OPS-001A,SEC-001,E2E-002R
SEC-005B|SEC-005A,SEC-002A,E2E-002R
SEC-006A|OPS-001A,SEC-001,E2E-002R
SEC-006B|SEC-002A,SEC-006A,E2E-002R
SEC-006C|SEC-001,DST-001
ADM-001|GOV-002,SEC-000,CAP-001A
SEC-007|ADM-001,SEC-002B,GWR-001,ENG-007,ENG-008,ENG-009,ENG-009A,ENG-010
SEC-008A|OPS-001B,DST-001
SEC-008B|GOV-001
SEC-009|SEC-000,SEC-002A,E2E-002R
SEC-010|SEC-002C,SEC-005B,SEC-007
CAP-001A|GOV-002
DST-001|GOV-004,OPS-001A,SEC-006A
DSR-001|DST-001
DSR-002|DSR-001,PGR-001,ENG-015
DSR-003|DSR-002,GWR-002,SEC-002B,SEC-005B
STO-001|DST-001,GOV-004,PROTO-002,E2E-001R
STO-002|DST-001,PROTO-002,PROTO-003A,E2E-001R
ENG-001|PROTO-001A,PROTO-001B
ENG-001A|PROTO-001A,ENG-001
ENG-002|PROTO-003A,ENG-007A
ENG-003|PROTO-002,E2E-001R
TSC-001|PROTO-002,ENG-003
ENG-004|GOV-004,E2E-001R
ENG-005|ENG-017,OPS-008
ENG-006|SEC-006A
ENG-006A|PROTO-003A,DST-001,SEC-004,CAP-001A
ENG-007A|ADM-001,PROTO-003A,CAP-001A,E2E-001R
ENG-007|ADM-001,CAP-001A,STO-001
ENG-008|ADM-001,ENG-007,CAP-001A
ENG-009|ADM-001,ENG-006A,CAP-001A
ENG-009A|ADM-001,PROTO-002,CAP-001A
ENG-010|ADM-001,DST-001,STO-001,STO-002,ENG-007A,CAP-001A
ENG-011|DST-001,DSR-002,STO-001,ENG-010
ENG-012|OPS-001A,DST-001,SEC-005A,SEC-006A,ADM-001
ENG-014|PROTO-002,E2E-001R
ENG-015|STO-001,PROTO-002,E2E-001R
ENG-017|GOV-002
LEAD-001|GOV-002,DST-001
OPS-001A|SEC-000,SEC-001
OPS-001B|OPS-001A,SEC-004,SEC-005A,SEC-005B,SEC-006A,ENG-012,LEAD-001
OPS-002|DSR-001,DSR-002,DSR-003,GWR-002,PGR-001,ENG-010,ENG-011,ENG-015
PG18-000|GOV-002
PG18-001A|PG18-000,E2E-000I,ENG-017
PG18-001B|PG18-001A
PG18-001Q|PG18-001A,PG18-001B,E2E-000I
PG18-002A|PG18-000,E2E-000I,ENG-006
PG18-002C|PG18-000,E2E-000I,ENG-006
OPS-003A|ENG-017,SEC-003B,SEC-006A
OPS-003B|OPS-003A,ENG-006,ENG-017,PG18-000,PG18-001Q,PG18-002A,PG18-002C
PGR-001|PG18-000,OPS-003A,OPS-003B
PG18-002B|PG18-002A,PG18-002C,DSR-003
PG18-003A|PG18-001Q,PG18-002A,PG18-002C,SEC-006A,OPS-003A,OPS-003B
PG18-004|PG18-003A,PGR-001,GOV-004
PG18-003Q|PG18-003A,PG18-002B,PG18-004,PGR-001,OPS-001B,LEAD-001,OPS-004
OPS-004|OPS-001B,OPS-002,OPS-003B,PG18-002C,LEAD-001,E2E-001R
OPS-005|GOV-004,DST-001,STO-001,ENG-004,ENG-015
OPS-006|CAP-001B,DST-001,SEC-010
OPS-007|OPS-002,OPS-004,OPS-005,OPS-006,SEC-006B
OPS-008|ENG-004,ENG-017,SEC-003,GOV-005
OPS-009|RLS-001,OPS-004,OPS-005,OPS-006,OPS-007,OPS-008,CAP-004,TST-003,TST-004,TST-005
CAP-000|GOV-002,CAP-001A
CAP-001B|CAP-000,ENG-006A,ENG-007,ENG-007A,ENG-008,ENG-009,ENG-009A,ENG-010,DST-001
CAP-002|CAP-000,CAP-001B
CAP-003A|CAP-000,CAP-001B,GOV-005
CAP-003Q|CAP-003A
CAP-004|CAP-003Q,OPS-002,OPS-004,TST-010,TST-011,TST-012
CAP-005|CAP-000,CAP-001B
CMP-001|CMP-000,GOV-002
APP-OWN-001|CMP-001
CMP-002|CMP-001,APP-OWN-001
CMP-002B|CMP-002,ENG-003,PROTO-002,TST-002V
CMP-003|CMP-002,SEC-002B,SEC-003,SEC-004,ENG-003
CMP-004|CMP-002B,CMP-003,APP-OWN-001,E2E-003CR
CMP-004A|CMP-004,E2E-003CR
CMP-005|CMP-004A,APP-OWN-001
CMP-006|CMP-005,TST-002C,TST-006C,TST-008C
SWF-000|NATIVE-ADR-001,PROTO-002,PROTO-003A,PROTO-004
SWF-001|NATIVE-ADR-001,GOV-004
SWF-002|SWF-001,PROTO-001A,PROTO-001D,PROTO-004,TST-002V,E2E-003NR
SWF-003A|SWF-001,PROTO-003A,E2E-003NR
SWF-003B|SWF-003A,SEC-004,SEC-009,PROTO-004,E2E-003NR
SWF-004|SWF-000,SWF-003B,SEC-002B,E2E-003NR
SWF-005A|SWF-004,SWF-002,E2E-003NR
SWF-005B|SWF-005A,PROTO-003B,ENG-002
SWF-006|SWF-005A,SWF-000,SWF-002,E2E-003NR
SWF-007|SWF-006
SWF-008|SWF-004,SWF-002,SWF-005A
SWF-009|ENG-001A,SWF-006
SWF-010|SWF-004,SWF-006,E2E-003NR
SWF-011|SWF-010,CAP-001A
SWF-012|SWF-010,SEC-006B,SEC-006C,E2E-003NR
SWF-013|SWF-002,SWF-003B,SWF-006,SWF-010,SWF-011,SWF-012,TST-002N,TST-006N,TST-008N
APP-NATIVE-CONSUMER-001|SWF-006,SWF-010,SWF-012,NATIVE-ADR-001
APP-NATIVE-SINK-001|SWF-007,APP-OWN-001,NATIVE-ADR-001
TST-000|PLAN-001
E2E-000S|PLAN-001,TST-000,PG18-000
E2E-000A|E2E-000S
E2E-000B|E2E-000S,TST-000,PG18-000
E2E-000I|E2E-000A,E2E-000B
TST-007|TST-000,CAP-000
TST-001|TST-000,ENG-003,ENG-004,ENG-014,ENG-015,ENG-017,STO-001,STO-002
TST-002A|PROTO-001A,PROTO-001B,PROTO-002,PROTO-003A,PROTO-004
TST-002V|PROTO-001A,PROTO-001C,PROTO-001D,PROTO-004
TST-002C|TST-002A,CMP-002B,CMP-003
TST-002N|TST-002A,SWF-002,SWF-003B
TST-004|SEC-002A,SEC-002B,SEC-002C,SEC-003,SEC-003B,SEC-004,SEC-005A,SEC-005B,SEC-006A,SEC-006B,SEC-006C,SEC-007,SEC-009,SEC-010,ENG-006A
TST-005|TST-001,ENG-003,TSC-001
TST-006C|CMP-004,CMP-005,TST-002C
TST-006N|SWF-003A,SWF-004,SWF-005A,SWF-006,SWF-010,SWF-012,TST-002N
TST-008C|TST-006C,CMP-005,SEC-006B
TST-008N|TST-006N,SWF-011,SWF-012
TST-010|TST-007,STO-001,STO-002,ENG-007,ENG-010,ENG-011,ENG-014,ENG-015
TST-011|TST-007,ENG-004,ENG-006,ENG-007A,ENG-009A,ENG-017
TST-012|TST-007,DSR-001,DSR-002,DSR-003,ENG-012,GWR-002,PGR-001,OPS-002,OPS-004,OPS-005
E2E-001R|E2E-000I,PROTO-002,PROTO-003A,PG18-000
E2E-001Q|E2E-001R,PG18-003Q,ENG-003,ENG-004,ENG-007A,ENG-014,ENG-015,STO-001,STO-002,OPS-001B,OPS-004,TST-010,TST-011,TST-012
E2E-002R|E2E-000I,OPS-001A,SEC-000,PROTO-001A,PROTO-001B,PROTO-002
E2E-002Q|E2E-002R,OPS-001B,GWR-002,SEC-002A,SEC-002B,SEC-002C,SEC-003,SEC-004,SEC-005A,SEC-005B,SEC-006A,SEC-006B,SEC-006C,SEC-007,SEC-009,SEC-010
E2E-003CR|E2E-000I,E2E-002Q,CMP-000,CMP-001,APP-OWN-001,CMP-002,CMP-002B
E2E-003CQ|E2E-003CR,E2E-001Q,CMP-004A,CMP-006,TST-006C,TST-008C
E2E-003NR|E2E-000I,E2E-002Q,SWF-000,SWF-001,TST-002V
E2E-003NQ|E2E-003NR,E2E-001Q,SWF-013,TST-006N,TST-008N
E2E-003T|E2E-003NQ,PROTO-003B,ENG-002,SWF-005B
E2E-003S|E2E-003NQ,SWF-007,APP-NATIVE-SINK-001
E2E-003A|E2E-003NQ,SWF-008
E2E-003U|E2E-003NQ,ENG-001,ENG-001A,SWF-009
E2E-004R|E2E-000I,MIG-000,MIG-001,APP-OWN-001
E2E-004Q|E2E-004R,MIG-002,MIG-002B,MIG-003
E2E-005|E2E-001Q,E2E-002Q,CAP-004,TST-010,TST-011,TST-012
TST-003|TST-001,TST-004,TST-005,TST-010,TST-011,TST-012,CAP-004,E2E-001Q,E2E-002Q,E2E-005
MIG-000|CMP-001,OPS-003A,OPS-008,E2E-000A
MIG-001|MIG-000,APP-OWN-001,TST-002A
MIG-002|MIG-001,APP-OWN-001,MIG-003,E2E-004R
MIG-002B|MIG-000,MIG-001,APP-OWN-001,E2E-004R
MIG-003|GOV-005,APP-OWN-001,SEC-002C,E2E-004R
MIG-004|MIG-001,MIG-002,MIG-002B,E2E-004Q,OPS-009,TST-003
MIG-005|MIG-004,MIG-002B
MIG-006|MIG-005,MIG-003,E2E-004Q
MIG-007|MIG-006
MIG-008|MIG-007
MIG-009|MIG-008`

const conditional = [
  ['CAP-005','C','TST-008C'],['CAP-005','N','TST-008N'],['SWF-013','T','SWF-005B'],['SWF-013','S','SWF-007'],['SWF-013','A','SWF-008'],['SWF-013','U','SWF-009'],
  ['E2E-003NQ','N&&!S','APP-NATIVE-CONSUMER-001'],['E2E-003NQ','N&&S','APP-NATIVE-SINK-001'],['E2E-004R','C','E2E-003CQ'],['E2E-004R','N&&!S','E2E-003NQ,APP-NATIVE-CONSUMER-001'],['E2E-004R','N&&S','E2E-003NQ,APP-NATIVE-SINK-001'],
  ['E2E-004Q','C','E2E-003CQ'],['E2E-004Q','N&&!S','E2E-003NQ,APP-NATIVE-CONSUMER-001'],['E2E-004Q','N&&S','E2E-003NQ,APP-NATIVE-SINK-001'],['E2E-005','C','E2E-003CQ'],['E2E-005','N','E2E-003NQ'],['E2E-005','T','E2E-003T'],['E2E-005','S','E2E-003S'],['E2E-005','A','E2E-003A'],['E2E-005','U','E2E-003U'],
  ['TST-003','C','TST-002C,TST-006C,TST-008C,CMP-006,CAP-002,CAP-005,E2E-003CQ'],['TST-003','N','TST-002N,TST-006N,TST-008N,SWF-013,CAP-005,E2E-003NQ'],['TST-003','N&&!S','APP-NATIVE-CONSUMER-001'],['TST-003','N&&S','APP-NATIVE-SINK-001'],['TST-003','T','E2E-003T'],['TST-003','S','E2E-003S'],['TST-003','A','E2E-003A'],['TST-003','U','E2E-003U'],
  ['MIG-001','C','CMP-002B'],['MIG-001','N','SWF-002'],['MIG-002','C','CMP-005'],['MIG-002','N&&!S','SWF-006,APP-NATIVE-CONSUMER-001'],['MIG-002','N&&S','SWF-006,SWF-007,APP-NATIVE-SINK-001'],['MIG-004','C','CMP-006'],['MIG-004','N&&!S','SWF-013,APP-NATIVE-CONSUMER-001'],['MIG-004','N&&S','SWF-013,APP-NATIVE-SINK-001'],
].map(([consumer, when, requires]) => ({ consumer, when, requires: requires.split(',') }))

const featureFor = (id: string): string | undefined => id.includes('SUBSET') || id === 'ENG-001' || id === 'ENG-001A' || id === 'SWF-009' || id === 'E2E-003U' ? 'U' : id.includes('AGGREGATE') || id === 'SWF-008' || id === 'E2E-003A' ? 'A' : id.includes('TXN') || id === 'ENG-002' || id === 'SWF-005B' || id === 'E2E-003T' ? 'T' : id.includes('SINK') || id === 'SWF-007' || id === 'E2E-003S' ? 'S' : undefined
const isNative = (id: string) => /^(SWF-|NATIVE-|APP-NATIVE|E2E-003N)/.test(id)
const expression = (id: string) => featureFor(id) ? `N&&${featureFor(id)}` : isNative(id) ? 'N' : id.startsWith('CMP-') || id === 'E2E-003CR' || id === 'E2E-003CQ' ? 'C' : 'true'
const profileExpression = (profiles: string) => profiles.includes('`COMPAT_V1`') ? 'C' : profiles.includes('`NATIVE_SUBSET`') ? 'N&&U' : profiles.includes('`NATIVE_AGGREGATE`') ? 'N&&A' : profiles.includes('`NATIVE_TXN_ATOMIC`') ? 'N&&T' : profiles.includes('`NATIVE_REPLICA_SINK`') ? 'N&&S' : /native/i.test(profiles) ? 'N' : 'true'
let definitionCache: Task[] | undefined
const definitions = (): Task[] => definitionCache ??= (() => {
  const authority = readFileSync(resolve(validatorRoot, authorityPins[0].path), 'utf8')
  return authority.split(/^### /m).slice(1).flatMap(section => {
    const [heading, ...rest] = section.split('\n'); const match = heading.match(/^([A-Z][A-Z0-9-]+) — (.+)$/); if (!match) return []
    const body = rest.join('\n'); const header = body.match(/\*\*Depends:\*\*([\s\S]*?)\.\s*\*\*Profiles:\*\*([\s\S]*?)\.\s*\*\*Boundary:\*\*\s*([^\n]+)/)
    const parsed = header ?? fail(`authority_task_parse:${match[1]}`)
    const [, dependencyText, profiles, boundary] = parsed; const id = match[1]; const direct = [...dependencyText.matchAll(/`([A-Z][A-Z0-9-]+)`/g)].map(entry => entry[1])
    const dependencies: Edge[] = direct.map(requires => /E2E-\d{3}.*R$/.test(requires) && !id.startsWith('E2E-') ? { requires, type: 'red_artifact', provider: requires, consumer: id, scenario_registry_requirement: 'registered_current_base_reviewed', scope: 'profile', base: 'current_integration_base', independent_review: true } : { requires, type: 'integrated' })
    const bootstrapPlanning = new Set(['PLAN-001','GOV-001','GOV-002','GOV-003','CMP-000','SEC-008B','TST-000'])
    const execution_scope = bootstrapPlanning.has(id) || /\b(all|COMMON_SERVER|all clients|all migration lanes|Swift client profiles)\b/.test(profiles) ? 'shared_producer' : 'per_profile'
    const proof_kind = id === 'TST-000' ? 'inherited_control' : bootstrapPlanning.has(id) || /governance|baseline|manifest|ADR/i.test(boundary) ? 'non_behavioral' : 'genuine_red'
    // Resource identities are generated from the task id and boundary, never from
    // prose placeholders.  Document-only tasks explicitly declare no runtime
    // reservation; executable tasks get a stable task-scoped reservation.
    const safe = boundary.trim().replace(/[^A-Za-z0-9._/-]+/g, '-').replace(/^-+|-+$/g, '') || id.toLowerCase()
    const concreteBoundary = `task/${id}/${safe}`
    const runtime = execution_scope === 'shared_producer' && proof_kind === 'non_behavioral'
      ? []
      : [`runtime/${id}/${safe}`]
    return [{ id, title: match[2], dependencies, applicability: profileExpression(profiles), execution_scope, proof_kind, owner: `owner:${boundary.trim()}`, principal_write_boundary: concreteBoundary, artifacts: [`artifact/${id}/${safe}`], read_resources: [authorityPins[0].path, authorityPins[1].path], write_resources: [`write/${id}/${safe}`], semantic_resources: [`semantic/${id}/${sha256(match[2]).slice(0, 16)}`], runtime_resources: runtime, scenario_ids: [], acceptance: body.match(/Acceptance:\s*([\s\S]*?)(?=\n### |$)/)?.[1].trim() ?? '' } as Task]
  })
})()

export const evaluate = (expression: string, profile: Profile): boolean => {
  const vars: Record<string, boolean> = { C: profile.lane === 'COMPAT_V1', N: profile.lane === 'NATIVE_CORE', A: profile.features.includes('NATIVE_AGGREGATE'), U: profile.features.includes('NATIVE_SUBSET'), T: profile.features.includes('NATIVE_TXN_ATOMIC'), S: profile.features.includes('NATIVE_REPLICA_SINK') }
  const permitted = ['NATIVE_AGGREGATE','NATIVE_SUBSET','NATIVE_TXN_ATOMIC','NATIVE_REPLICA_SINK']
  if (profile.features.some(x => !permitted.includes(x)) || new Set(profile.features).size !== profile.features.length || (profile.features.length && !vars.N)) fail('invalid_profile')
  const compact = expression.replace(/\s/g, '')
  const tokens = compact.match(/&&|\|\||true|false|[CNAUST]|[!()]/g) ?? []
  if (!compact || tokens.join('') !== compact) fail('invalid_profile_expression')
  let position = 0
  const atom = (): boolean => {
    const token = tokens[position++]
    if (token === '!') return !atom()
    if (token === '(') { const value = or(); if (tokens[position++] !== ')') fail('invalid_profile_expression'); return value }
    if (token === 'true') return true
    if (token === 'false') return false
    if (token in vars) return vars[token]
    return fail('invalid_profile_expression')
  }
  const and = (): boolean => { let value = atom(); while (tokens[position] === '&&') { position++; value = atom() && value } return value }
  const or = (): boolean => { let value = and(); while (tokens[position] === '||') { position++; value = and() || value } return value }
  const value = or()
  if (position !== tokens.length) fail('invalid_profile_expression')
  return value
}
const dependencyEdges = (task: any): Edge[] => Array.isArray(task.dependencies) ? task.dependencies : [...task.dependencies.integrated.map((requires: string) => ({ requires, type: 'integrated' as const })), ...task.dependencies.red_artifacts]
export const manifest = () => ({ version: 1, canonicalization: 'json-sort-keys-v1', authoritative_inputs: [{ path: 'notes/18-production-readiness-spec-reviewed.md', blob_sha: '5b02585cf7224563dc7246ba310e5621d8b98b67' }, { path: 'notes/skills-research/05-parallel-agent-execution-protocol.md', blob_sha: '29d84268de4a6cf5e12868ba5738a85c624f095c' }], future_inputs: { release_profiles: { kind: 'unavailable', owner: 'GOV-005' }, scenario_registry: { kind: 'unavailable', owner: 'E2E-000S' } }, tasks: definitions().map(task => { const taskConditional = conditional.filter(edge => edge.consumer === task.id); const conditionalRequires = new Set(taskConditional.flatMap(edge => edge.requires)); return { ...task, artifacts: task.id === 'PLAN-001' ? ['docs/production/readiness-tasks.json','docs/production/readiness-task.schema.json','docs/production/readiness-gates.json','docs/production/readiness-plan.generated.md','scripts/readiness-plan.ts','scripts/readiness-plan.test.ts'] : task.artifacts, resources: { read: task.read_resources, write: task.write_resources, semantic: task.semantic_resources, runtime: task.runtime_resources }, dependencies: { integrated: task.dependencies.filter(edge => edge.type === 'integrated' && !conditionalRequires.has(edge.requires)).map(edge => edge.requires), conditional: taskConditional.map(edge => ({ when: edge.when, requires: edge.requires })), red_artifacts: task.dependencies.filter(edge => edge.type === 'red_artifact') } } }), conditional_edges: conditional })
export const validateManifest = (raw: Json): void => {
  validateAuthorities()
  if (raw.version !== 1 || !Array.isArray(raw.tasks) || raw.tasks.length !== 169 || !Array.isArray(raw.conditional_edges) || raw.conditional_edges.length !== 36) fail('invalid_manifest_shape')
  const tasks = raw.tasks as any[]; const ids = new Set<string>()
  for (const task of tasks) {
    if (!/^[A-Z][A-Z0-9-]+$/.test(task.id) || ids.has(task.id)) fail('duplicate_or_invalid_task')
    ids.add(task.id)
    if (!task.principal_write_boundary) fail('invalid_principal_boundary')
    if (!task.execution_scope || !task.applicability || !Array.isArray(task.artifacts) || !Array.isArray(task.semantic_resources)) fail('invalid_task_shape')
    if (/\b(TBD|latest)\b/i.test(JSON.stringify(task))) fail('invalid_mutable_placeholder')
    if (String(task.principal_write_boundary).startsWith('boundary:') || task.artifacts.some((x: unknown) => String(x).startsWith('boundary:')) || task.write_resources?.some((x: unknown) => String(x).startsWith('boundary:'))) fail('noncanonical_task_inventory')
    if (!task.artifacts.length || !task.write_resources?.length || !task.semantic_resources.length) fail('missing_task_resource_identity')
  }
  for (const task of tasks) for (const dependency of dependencyEdges(task)) { if (dependency.type !== 'integrated' && dependency.type !== 'red_artifact') fail('invalid_edge_type'); if (!ids.has(dependency.requires)) fail('unknown_dependency'); if (dependency.type === 'red_artifact' && (!dependency.provider || dependency.consumer !== task.id || dependency.scope !== 'profile' || dependency.base !== 'current_integration_base' || !dependency.independent_review)) fail('invalid_red_artifact_edge') }
  for (const edge of raw.conditional_edges as { consumer: string; when: string; requires: string[] }[]) { if (!ids.has(edge.consumer) || !edge.requires.every(id => ids.has(id))) fail('unknown_conditional_dependency'); evaluate(edge.when, { lane: 'NATIVE_CORE', features: [] }) }
  const embeddedConditions = tasks.flatMap((task: any) => task.dependencies?.conditional?.map((edge: any) => ({ consumer: task.id, when: edge.when, requires: edge.requires })) ?? []); if (canonical(embeddedConditions) !== canonical(conditional)) fail('invalid_conditional_inventory')
  for (const task of tasks) for (const requires of task.dependencies.integrated ?? []) if (tasks.find(other => other.id === requires)?.applicability === 'false') fail('required_inapplicable_dependency')
  const artifacts = new Set<string>(), semantic = new Set<string>(), runtime = new Set<string>()
  for (const task of tasks) {
    for (const artifact of task.artifacts) { if (artifacts.has(artifact)) fail('duplicate_artifact_owner'); artifacts.add(artifact) }
    for (const resource of task.semantic_resources) { if (semantic.has(resource)) fail('duplicate_semantic_resource_owner'); semantic.add(resource) }
    for (const resource of task.runtime_resources) { if (typeof resource !== 'string' || runtime.has(resource)) fail('duplicate_runtime_resource_owner'); runtime.add(resource) }
    if (task.runtime_resources.some((x: unknown) => typeof x === 'string' && (x === 'runtime:generic' || x.startsWith('runtime:generic')))) fail('generic_runtime_resource')
  }
  const visiting = new Set<string>(), done = new Set<string>(), byId = new Map(tasks.map(task => [task.id, task]))
  const visit = (id: string) => { if (visiting.has(id)) fail('cyclic_dependency'); if (done.has(id)) return; visiting.add(id); for (const dependency of dependencyEdges(byId.get(id)!)) if (dependency.type === 'integrated') visit(dependency.requires); visiting.delete(id); done.add(id) }
  tasks.forEach(task => visit(task.id))
  if (canonical(tasks) !== canonical(manifest().tasks)) fail('noncanonical_task_inventory')
  if (canonical(raw.conditional_edges) !== canonical(conditional)) fail('noncanonical_conditional_inventory')
}
export const selectedDependencies = (raw: ReturnType<typeof manifest>, task: any, profile: Profile) => [...new Set([...dependencyEdges(task).filter(edge => edge.type === 'integrated').map(edge => edge.requires), ...task.dependencies.conditional.filter((edge: { when: string }) => evaluate(edge.when, profile)).flatMap((edge: { requires: string[] }) => edge.requires)])]
const readyValidated = new WeakSet<object>()
export const ready = (raw: ReturnType<typeof manifest>, profile: Profile | null, completed: Set<string>, scope: string) => {
  if (!readyValidated.has(raw)) { validateManifest(raw); readyValidated.add(raw) }
  if (scope !== 'shared-pre-registry' && !profile) fail('profile_required');
  return raw.tasks.filter(task => task.id !== 'PLAN-001' && !completed.has(task.id) && (scope === 'shared-pre-registry' ? task.execution_scope === 'shared_producer' && task.proof_kind !== 'genuine_red' : evaluate(task.applicability, profile!)) && selectedDependencies(raw, task, profile ?? { lane: 'NATIVE_CORE', features: [] }).every(id => completed.has(id))).map(task => task.id).sort()
}
type Outcome = 'pass' | 'fail' | 'blocked' | 'not_applicable_by_profile'
export const scheduledReady = (raw: ReturnType<typeof manifest>, profile: Profile, outcomes: Map<string, Outcome>, reviewedRed: Set<string>) => raw.tasks.filter(task => evaluate(task.applicability, profile) && task.id !== 'PLAN-001' && !outcomes.has(task.id)).filter(task => {
  if (!task.dependencies.red_artifacts.every(edge => reviewedRed.has(`${edge.provider}:${edge.consumer}`))) return false
  return selectedDependencies(raw, task, profile).every(requires => {
    const dependency = raw.tasks.find(item => item.id === requires) ?? fail('unknown_dependency')
    if (!evaluate(dependency.applicability, profile)) fail('required_inapplicable_dependency')
    return outcomes.get(requires) === 'pass'
  })
}).map(task => task.id).sort()
export const validateOutcome = (task: any, profile: Profile, outcome: Outcome) => {
  validateOperational({ task_id: task.id, profile, outcome, state: outcome === 'pass' ? 'qualified' : 'declared' }, 'outcome', 'outcome')
  if (outcome === 'not_applicable_by_profile' && evaluate(task.applicability, profile)) fail('required_not_applicable')
  if (outcome !== 'pass' && outcome !== 'not_applicable_by_profile') fail('nonterminal_predecessor')
}
const exactBootstrapOutputs = ['docs/production/readiness-tasks.json','docs/production/readiness-task.schema.json','docs/production/readiness-gates.json','docs/production/readiness-plan.generated.md','scripts/readiness-plan.ts','scripts/readiness-plan.test.ts']
const bootstrapPacketHash = (packet: Json): string => {
  const projection = JSON.parse(JSON.stringify(packet)) as Json
  const live = projection.control as Json | undefined
  if (live) { delete live.packet_sha256; delete live.lease_expires_at; delete live.controller_event_log_ref }
  return sha256(canonical(projection))
}
const validateBootstrapV2 = (packet: Json) => {
  const authority = packet.authority as Json, scope = packet.execution_scope as Json, ownership = packet.ownership as Json, contract = packet.contract as Json, control = packet.control as Json, workspace = packet.workspace as Json, handoff = packet.handoff as Json, gitAuthority = packet.git_authority as Json, base = packet.base as Json, future = packet.future_inputs as Json, evidence = packet.evidence_contract as Json, review = packet.review as Json
  if (packet.packet_version !== 2 || packet.packet_kind !== 'bootstrap_plan' || packet.task_id !== 'PLAN-001' || packet.attempt !== 10) fail('invalid_bootstrap_packet')
  if (scope?.kind !== 'bootstrap' || scope.id !== 'bootstrap-plan-001' || scope.profile_scope !== 'uncompiled_all' || packet.profile !== null || packet.release_profile_hash !== null) fail('invalid_bootstrap_scope')
  const forbiddenAuthorities = ['all files from PLAN-001 attempts a1 through a8','notes/16-production-readiness-spec.md','notes/reviews/** as planning authority','controller or author checkout evidence']
  const requiredWorkspace = ['worktree','branch','run_root','dependency_snapshot','dependency_content_sha256','dependency_manifest_sha256','resolver_profile','resolver_config_sha256','mount_topology_sha256','mount_attestation_sha256']
  const directGates = ['node --import tsx --test scripts/readiness-plan.test.ts','node --import tsx --test /tmp/electric-circuits-plan-001-a4-regression.test.ts','pnpm exec tsc --ignoreConfig --noEmit --strict --target ES2022 --module NodeNext --moduleResolution NodeNext --types node scripts/readiness-plan.ts scripts/readiness-plan.test.ts','pnpm exec tsx scripts/readiness-plan.ts validate','pnpm exec tsx scripts/readiness-plan.ts ready --scope shared-pre-registry --completed PLAN-001','pnpm exec tsx scripts/readiness-plan.ts ready --scope shared-pre-registry --completed PLAN-001,GOV-001','pnpm typecheck']
  const authorityInputs = authority?.canonical_inputs ?? authority?.planning_input_allowlist
  if (authority?.integration_commit !== integrationCommit || (authority.integration_tree ?? authority.initial_tree_sha) !== integrationTree || !Array.isArray(authorityInputs) || !authorityInputs.some((x: Json) => x.path === authorityPins[0].path && (x.blob_sha ?? x.git_blob_sha) === authorityPins[0].blob) || !authorityInputs.some((x: Json) => x.path === authorityPins[1].path && (x.blob_sha ?? x.git_blob_sha) === authorityPins[1].blob)) fail('invalid_bootstrap_authority')
  const expectedForbidden = ['notes/16-production-readiness-spec.md','notes/reviews/**','notes/23-swift-app-e2e-tdd-map.md','notes/24-postgres18-and-e2e-tdd-addendum.md','notes/25-pg18-e2e-differential-review-disposition.md','all PLAN-001 attempt artifacts a1 through a9','all controller, author, reviewer, cache, generated-output, and mutable checkout evidence']
  const forbidden = authority?.forbidden_as_planning_authority ?? authority?.forbidden_inputs
  if (canonical(forbidden) !== canonical(expectedForbidden)) fail('invalid_bootstrap_authority')
  const allowed = ownership?.allowed_outputs ?? ownership?.allowed_paths
  const expectedAllowed = exactBootstrapOutputs.slice(0, 6).concat(['notes/execution/PLAN-001/bootstrap-plan-001/a10.md'])
  if (canonical(allowed) !== canonical(expectedAllowed)) fail('invalid_bootstrap_ownership')
  if (!Array.isArray(ownership?.forbidden ?? ownership?.forbidden_paths) || !(workspace?.author_worktree || workspace?.worktree)) fail('invalid_bootstrap_ownership')
  if (contract?.authorities_only !== true && !Array.isArray(contract?.required_deliverables)) fail('invalid_bootstrap_contract')
  if (contract?.authorities_only === true && (contract.exact_task_inventory !== 169 || contract.normative_conditional_groups !== 36 || contract.normative_conditional_requirements !== 56 || canonical(packet.direct_gates) !== canonical(directGates))) fail('invalid_bootstrap_contract')
  const ttl = Number(control?.lease_ttl_secs), interval = Number(control?.heartbeat_interval_secs)
  if (!control || typeof control.scheduler_generation !== 'number' || !control.reservation_lease_id || !Number.isFinite(ttl) || ttl < 30 || ttl > 300 || !Number.isFinite(interval) || interval > ttl / 3 || !control.heartbeat_auth_ref || !control.lease_expires_at || !control.request_mailbox || !control.ack_mailbox || !control.phase_file || !control.stop_file) fail('invalid_bootstrap_control')
  if (packet.attempt === 10 && (typeof control.packet_sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(control.packet_sha256 as string))) fail('packet_hash_mismatch')
  if (packet.attempt === 10 && control.packet_sha256 !== bootstrapPacketHash(packet)) fail('packet_hash_mismatch')
  const eventLog = control.controller_event_log_ref as string | undefined
  if (packet.attempt === 10 && (!eventLog || !existsSync(eventLog))) fail('packet_lease_binding')
  if (eventLog && existsSync(eventLog)) {
    const admitted = readFileSync(eventLog, 'utf8').split('\n').map(line => { try { return JSON.parse(line) as Json } catch { return null } }).find(row => row?.event === 'reservation_acquired')
    if (admitted && (admitted.packet_sha256 !== control.packet_sha256 || admitted.reservation_lease_id !== control.reservation_lease_id || admitted.generation !== control.scheduler_generation)) fail('packet_lease_binding')
  }
  if (gitAuthority && (gitAuthority.mode !== 'commit_only' && gitAuthority.mode !== 'prepare_patch' || gitAuthority.authority_ref !== 'user-2026-08-23-kick-off-first-set')) fail('invalid_bootstrap_handoff')
  const generated = exactBootstrapOutputs.slice(0, 6)
  const releaseProfiles = future?.release_profiles as Json | undefined, scenarioRegistry = future?.scenario_registry as Json | undefined
  const declared = packet.declared_outputs as Json[] | undefined
  const packetOutputsValid = Array.isArray(declared) && canonical(declared.map(row => String(row.path)).sort()) === canonical([...generated].sort()) && declared.every(row => (row.identity === null || row.base_git_identity === 'absent') && (row.owner_task === 'PLAN-001' || row.owner === 'PLAN-001'))
  if (!base?.initial_head || !(base.initial_tree ?? base.initial_tree_sha) || base.initial_head !== integrationCommit || (base.initial_tree ?? base.initial_tree_sha) !== integrationTree || !packetOutputsValid || (releaseProfiles?.state !== 'unavailable' && releaseProfiles?.kind !== 'unavailable') || releaseProfiles.owner !== 'GOV-005' || (scenarioRegistry?.state !== 'unavailable' && scenarioRegistry?.kind !== 'unavailable') || scenarioRegistry.owner !== 'E2E-000S') fail('invalid_bootstrap_packet_contract')
  if ('output_identities' in packet || 'release_profile_identity' in packet || 'scenario_registry_identity' in packet) fail('bootstrap_future_identity_forbidden')
  const execution = packet.execution as Json | undefined
  if (packet.attempt !== 10) return
  const matrix = execution?.gate_matrix as Json | undefined
  if (!matrix || matrix.kind !== 'declared_output_absent_at_reservation' || matrix.path !== 'docs/production/readiness-gates.json' || matrix.identity_state !== 'pending_generation_output' || matrix.git_blob_sha !== null || matrix.canonical_sha256 !== null) fail('invalid_gate_matrix_binding')
  const bootstrapGates = execution?.bootstrap_direct_gates as Json[] | undefined
  const gates = Array.isArray(bootstrapGates) ? bootstrapGates : []
  const expectedGateIds = ['PLAN-001-UNIT','PLAN-001-TSC','PLAN-001-ROOT-TYPECHECK','PLAN-001-BOOTSTRAP-VALIDATE','PLAN-001-GENERATED-VALIDATE','PLAN-001-READY-POST-BOOTSTRAP','PLAN-001-READY-AFTER-GOV-001']
  if (!Array.isArray(bootstrapGates) || gates.length !== expectedGateIds.length || canonical(gates.map(row => row.gate_id)) !== canonical(expectedGateIds) || gates.some(row => !Array.isArray(row.command_argv) || !row.acceptance || row.phase !== 'author_direct_and_merge_direct')) fail('invalid_bootstrap_gate_contract')
  const expectedCommands = [['node','--import','tsx','--test','scripts/readiness-plan.test.ts'],['pnpm','exec','tsc','--ignoreConfig','--noEmit','--strict','--target','ES2022','--module','NodeNext','--moduleResolution','NodeNext','--types','node','scripts/readiness-plan.ts','scripts/readiness-plan.test.ts'],['pnpm','typecheck'],['pnpm','exec','tsx','scripts/readiness-plan.ts','validate','--bootstrap-packet'],['pnpm','exec','tsx','scripts/readiness-plan.ts','validate','--manifest','docs/production/readiness-tasks.json','--schema','docs/production/readiness-task.schema.json','--gates','docs/production/readiness-gates.json'],['pnpm','exec','tsx','scripts/readiness-plan.ts','ready','--scope','shared-pre-registry','--completed','PLAN-001'],['pnpm','exec','tsx','scripts/readiness-plan.ts','ready','--scope','shared-pre-registry','--completed','PLAN-001,GOV-001']]
  if (gates.some((row, i) => canonical(row.command_argv) !== canonical(expectedCommands[i]))) fail('invalid_bootstrap_gate_command')
}
const sharedScopeId = (raw: ReturnType<typeof manifest>, task: any) => `shared-${sha256(canonical({ task: task.id, predecessors: selectedDependencies(raw, task, { lane: 'NATIVE_CORE', features: [] }) }))}`
const packetCoreSha256 = (packet: Json) => { const { control, core_sha256: _core, ...core } = packet; const lease = control as Json | undefined; const immutableControl = lease ? { scheduler_generation: lease.scheduler_generation, reservation_lease_id: lease.reservation_lease_id, ttl_ms: lease.ttl_ms, heartbeat_interval_ms: lease.heartbeat_interval_ms, heartbeat_auth_ref: lease.heartbeat_auth_ref, expires_at: lease.expires_at } : undefined; return sha256(canonical({ ...core, control: immutableControl })) }
const redArtifactKey = (artifact: Json) => canonical({ identity: artifact.identity, provider: artifact.provider_task, consumer: artifact.consumer_task, scenario: artifact.scenario_id, profile: artifact.profile_scope, base: artifact.base_sha })
export const validateRedArtifactAdmission = (raw: ReturnType<typeof manifest>, taskId: string, registry: Json | null, artifact: Json | null, consumed: Set<string>) => {
  if (!registry || !artifact) fail('scenario_registry_unavailable')
  const registered = registry as Json, reviewed = artifact as Json
  if (registered.kind !== 'registered' || typeof registered.identity !== 'string' || !Array.isArray(registered.scenarios)) fail('invalid_red_registry_binding')
  // v2.1 uses semantic_contract_sha256/scope; accept the earlier aliases only
  // for backwards-compatible focused tests, while requiring all binding fields.
  const semantic = (reviewed.semantic_contract_sha256 ?? reviewed.semantic_hash) as string | undefined
  const profileScope = (reviewed.profile_scope ?? reviewed.scope) as string | undefined
  if (typeof reviewed.identity !== 'string' || typeof reviewed.provider_task !== 'string' || typeof reviewed.consumer_task !== 'string' || typeof reviewed.scenario_id !== 'string' || typeof semantic !== 'string' || typeof profileScope !== 'string' || typeof reviewed.base_sha !== 'string' || reviewed.review_state !== 'red_proved' || typeof reviewed.author_id !== 'string' || typeof reviewed.reviewer_id !== 'string') fail('invalid_red_artifact')
  const task = raw.tasks.find(item => item.id === taskId) ?? fail('unknown_packet_task'), currentHead = checkoutIdentity().head
  if (task.proof_kind !== 'genuine_red' || reviewed.consumer_task !== taskId || reviewed.base_sha !== currentHead || reviewed.author_id === reviewed.reviewer_id || typeof reviewed.red_patch_sha !== 'string' || typeof (reviewed.red_evidence_sha256 ?? reviewed.red_evidence_sha) !== 'string') fail('invalid_red_artifact')
  if (!(registered.scenarios as Json[]).some(row => {
    const rowSemantic = (row as Json).semantic_contract_sha256 ?? (row as Json).semantic_hash
    return (row as Json).scenario_id === reviewed.scenario_id && rowSemantic === semantic && typeof (row as Json).test_owner_task === 'string'
  })) fail('invalid_red_registry_binding')
  if (!task.dependencies.red_artifacts.some(edge => edge.provider === reviewed.provider_task && edge.consumer === taskId) || consumed.has(redArtifactKey(reviewed))) fail('red_artifact_reused')
}
export const buildPlanningPacket = (raw: ReturnType<typeof manifest>, taskId: string, controllerState?: Json, control?: Json, attempt = 1) => {
  validateManifest(raw); const task = raw.tasks.find(item => item.id === taskId) ?? fail('unknown_packet_task')
  if (!controllerState) fail('controller_state_required')
  if (!control) fail('controller_lease_required')
  const issuedControl = control as Json
  validateOperational(controllerState, 'controller_state', 'controller_state')
  const state = controllerState as Json, currentHead = state.integration_head as string, currentTree = state.integration_tree as string, checkedOut = checkoutIdentity()
  if (currentHead !== checkedOut.head || currentTree !== checkedOut.tree) fail('stale_controller_state')
  if (canonical(state.lease) !== canonical(issuedControl)) fail('unrelated_controller_lease')
  validateLease(issuedControl, Date.now(), issuedControl.acknowledged_nonce as number)
  const controllerCompleted = new Set((state.resolutions as Json[]).filter(row => { const resolved = raw.tasks.find(task => task.id === row.task_id); const expectedScope = row.task_id === 'PLAN-001' ? 'bootstrap-plan-001' : resolved ? sharedScopeId(raw, resolved) : ''; return row.outcome === 'pass' && row.state === 'integrated' && row.generation === state.generation && row.scope_id === expectedScope && (row.base as Json).head === currentHead && (row.base as Json).tree === currentTree }).map(row => row.task_id as string))
  if (task.execution_scope !== 'shared_producer') fail('release_profile_unavailable')
  if (task.proof_kind === 'genuine_red') fail('scenario_registry_unavailable')
  if (!ready(raw, null, controllerCompleted, 'shared-pre-registry').includes(taskId)) fail('premature_task_packet')
  const identities = outputIdentities()
  const dispatchHead = currentHead, dispatchTree = currentTree
  const core: Json = { packet_version: 2, packet_kind: 'task', task_id: task.id, attempt, execution_scope: { kind: 'shared_producer', id: sharedScopeId(raw, task), profile_scope: 'shared' }, profile: null, predecessors: selectedDependencies(raw, task, { lane: 'NATIVE_CORE', features: [] }), topology: { proof_kind: task.proof_kind, red_artifact_input: null }, contract: { scenario_registry_identity: null, scenario_registry: 'not_applicable_pre_registry' }, base: { initial_head: dispatchHead, integration_tree: dispatchTree }, output_identities: identities.outputs, output_bundle_sha256: identities.bundle_sha256, authority: { integration_commit: dispatchHead, integration_tree: dispatchTree, canonical_inputs: raw.authoritative_inputs } }
  const core_sha256 = packetCoreSha256({ ...core, control: issuedControl }), boundControl = { ...issuedControl, packet_sha256: core_sha256 }
  return { ...core, core_sha256, control: boundControl } as any
}
export const validatePlanningPacket = (packet: Json, raw: ReturnType<typeof manifest>) => {
  validateOperational(packet, 'ordinary_packet', 'ordinary_packet'); const task = raw.tasks.find(item => item.id === packet.task_id) ?? fail('unknown_packet_task'); const scope = packet.execution_scope as Json
  if (scope.kind !== 'shared_producer' || packet.profile !== null || task.execution_scope !== 'shared_producer') fail('invalid_planning_scope')
  if (task.proof_kind === 'genuine_red') fail('scenario_registry_unavailable')
  const control = packet.control as Json; validateLease(control, Date.now(), control.acknowledged_nonce as number); if (packet.core_sha256 !== packetCoreSha256(packet) || control.packet_sha256 !== packet.core_sha256) fail('packet_lease_binding')
  const predecessors = selectedDependencies(raw, task, { lane: 'NATIVE_CORE', features: [] }), expectedScope = sharedScopeId(raw, task), base = packet.base as Json, authority = packet.authority as Json, currentHead = authority?.integration_commit as string, currentTree = authority?.integration_tree as string, checkedOut = checkoutIdentity()
  if (canonical(packet.predecessors) !== canonical(predecessors) || scope.id !== expectedScope || scope.profile_scope !== 'shared') fail('forged_planning_scope')
  if (currentHead !== checkedOut.head || currentTree !== checkedOut.tree || base.initial_head !== currentHead || base.integration_tree !== currentTree) fail('stale_dispatch_base')
  const actual = outputIdentities(); if (canonical(packet.output_identities) !== canonical(actual.outputs) || packet.output_bundle_sha256 !== actual.bundle_sha256 || canonical(authority.canonical_inputs) !== canonical(raw.authoritative_inputs)) fail('stale_output_identity')
}
export const validatePacket = (packet: Json, raw: ReturnType<typeof manifest>, registry: Json | null = null, consumedRed = new Set<string>()) => {
  if (packet.packet_kind === 'bootstrap_plan' && packet.packet_version === 2) { validateBootstrapV2(packet); return }
  if (packet.packet_kind === 'bootstrap_plan') fail('invalid_bootstrap_packet')
  if (packet.packet_kind === 'task' && (packet.contract as Json)?.scenario_registry === 'not_applicable_pre_registry' && !packet.candidate_identity) { validatePlanningPacket(packet, raw); return }
  // Full task packets carry the immutable execution/evidence/ownership contract.
  // Validate the discriminators here before applying the compact schema used by
  // planning packets; this prevents accepting a label-only or partial packet.
  if (packet.packet_kind === 'task' && packet.packet_version === 2 && packet.candidate_identity && packet.execution && packet.ownership && packet.deliverables) {
    const task = raw.tasks.find(item => item.id === packet.task_id) ?? fail('unknown_packet_task')
    const scope = packet.execution_scope as Json
    if (scope?.kind !== task.execution_scope || !packet.base || !packet.topology || !packet.contract || !packet.control || !packet.authority) fail('invalid_task_packet_contract')
    const checkedOut = checkoutIdentity(), fullBase = packet.base as Json, fullAuthority = packet.authority as Json
    if (fullBase.initial_head !== checkedOut.head || (fullBase.initial_tree_sha ?? fullBase.integration_tree) !== checkedOut.tree || fullAuthority.integration_commit !== checkedOut.head || fullAuthority.integration_tree !== checkedOut.tree) fail('stale_dispatch_base')
    const topology = packet.topology as Json; if (topology.proof_kind !== task.proof_kind) fail('packet_proof_mismatch')
    if (task.execution_scope === 'per_profile' && !packet.profile) fail('release_profile_unavailable')
    if (task.execution_scope === 'per_profile' && !(packet as Json).release_profile_hash) fail('release_profile_unavailable')
    const contract = packet.contract as Json
    if (topology.proof_kind === 'genuine_red') {
      if (!registry || contract.scenario_registry_identity !== registry.identity) fail('scenario_registry_unavailable')
      const artifact = topology.red_artifact_input as Json
      if (!artifact) fail('red_artifact_missing')
      const ledger = consumedRed as unknown as Json
      const consumed = ledger && Array.isArray(ledger.consumed_red_artifacts) ? new Set((ledger.consumed_red_artifacts as Json[]).map(redArtifactKey)) : consumedRed
      validateRedArtifactAdmission(raw, packet.task_id as string, registry, artifact, consumed)
      if (!Array.isArray(contract.scenario_ids) || !contract.scenario_ids.includes(artifact.scenario_id as string)) fail('invalid_red_registry_binding')
    } else if (contract.scenario_registry !== 'not_applicable_pre_registry' && topology.proof_kind === 'non_behavioral') fail('invalid_scenario_registry_state')
    const control = packet.control as Json; if (typeof control.scheduler_generation !== 'number' || !control.reservation_lease_id) fail('invalid_lease')
    if (control.packet_sha256 && packet.packet_sha256 && control.packet_sha256 !== packet.packet_sha256) fail('packet_lease_binding')
    return
  }
  validateOperational(packet, 'ordinary_packet', 'ordinary_packet')
  const id = packet.task_id; const task = raw.tasks.find(item => item.id === id) ?? fail('unknown_packet_task'); const scope = packet.execution_scope as Json; if (scope?.kind !== task.execution_scope) fail('execution_scope_mismatch')
  const proof = (packet.topology as Json)?.proof_kind; if (proof !== task.proof_kind) fail('packet_proof_mismatch'); if (proof === 'genuine_red') { if ((packet.contract as Json)?.scenario_registry_identity !== registry?.identity) fail('scenario_registry_unavailable'); validateRedArtifactAdmission(raw, id as string, registry, (packet.topology as Json)?.red_artifact_input as Json, consumedRed) }
  const lease = packet.control as Json; if (!lease?.heartbeat_auth_ref || !lease?.reservation_lease_id || typeof lease.scheduler_generation !== 'number') fail('invalid_lease')
}
export const validateEvidence = (row: Json) => { validateOperational(row, 'evidence', 'evidence'); if (row.source_strategy === 'author_checkout') fail('author_control_evidence'); if (row.source_strategy === 'control_checkout') fail('control_checkout_evidence'); const violations: [string, boolean][] = [['writable_overlay', row.overlay_writable === true], ['reused_run_root', row.run_root_reused === true], ['dirty_source', row.source_dirty === true || row.source_clean !== true], ['post_source_mutation', row.post_source_mutated === true], ['effective_config_mismatch', row.config_mismatch === true || row.config_matches !== true], ['external_input_mutated', row.external_input_mutated === true || row.external_inputs_unchanged !== true], ['nonempty_run_root', row.run_root_new_empty !== true], ['writable_mount', row.mount_read_only !== true], ['post_run_source_changed', row.post_source_unchanged !== true]]; for (const [reason, violated] of violations) if (violated) fail(reason) }
export const validateGates = (raw: Json, gates: Json) => { if (!gates.evidence_requirements || typeof gates.evidence_requirements !== 'object' || Array.isArray(gates.evidence_requirements) || !(gates.evidence_requirements as Json).source_strategy) fail('invalid_evidence_requirements'); const rows = gates.gates as Json[]; if (!Array.isArray(rows) || rows.length !== (raw.tasks as any[]).length * 3) fail('duplicate_gate_or_omission'); const expected = new Set((raw.tasks as any[]).flatMap(task => ['author_direct','merge_direct','final_release_qualification'].map(phase => `${task.id}:${phase}`))); for (const row of rows) { validateOperational(row, 'gate', 'gate'); const id = row.id as string, expectedPhase = id.split(':').at(-1); if (!expected.delete(id)) fail('duplicate_gate_or_move'); if (row.phase !== expectedPhase) fail('gate_phase_moved'); const command = String(row.command), planTest = row.task_id === 'PLAN-001' && command.includes('readiness-plan.test.ts'); if (/^(true|:|echo)(?:\s|$)/.test(command) || (!planTest && !command.includes(String(row.task_id))) || !row.config_identity || !Array.isArray(row.evidence_fields)) fail('task_specific_gate_command'); const task = (raw.tasks as any[]).find(item => item.id === row.task_id) ?? fail('invalid_gate_owner_or_profile'); if (row.owner !== task.owner || row.applicability !== task.applicability) fail('invalid_gate_owner_or_profile'); if (row.baseline_assertion && (row.task_id !== 'TST-000' || (row.baseline_assertion as Json).owner !== task.owner)) fail('invalid_gate_baseline_exception') } if (expected.size) fail('duplicate_gate_or_omission') }
export const validateLease = (lease: Json, now: number, acknowledgedNonce: number) => { validateOperational(lease, 'lease', 'lease'); if (!lease.authenticated || typeof lease.generation !== 'number' || lease.generation !== lease.current_generation) fail('stale_generation'); if ((lease.ttl_ms as number) < 30_000 || (lease.ttl_ms as number) > 300_000) fail('invalid_lease_ttl'); if ((lease.heartbeat_interval_ms as number) > (lease.ttl_ms as number) / 3) fail('invalid_heartbeat_interval'); if (!lease.control_plane_available) fail('control_plane_loss'); if ((lease.heartbeat_nonce as number) > acknowledgedNonce || (lease.heartbeat_nonce as number) > (lease.acknowledged_nonce as number)) fail('silent_renewal'); if ((lease.expires_at as number) <= now || (lease.last_heartbeat_at as number) + (lease.heartbeat_deadline_ms as number) < now) fail('missed_heartbeat') }
export const validateAgainstSchema = (value: unknown, schema: Json, where = '$', root: Json = schema): void => {
  if (Array.isArray(schema.anyOf)) {
    const accepted = (schema.anyOf as Json[]).some(candidate => { try { validateAgainstSchema(value, candidate, where, root); return true } catch { return false } })
    if (!accepted) fail(`schema_any_of:${where}`)
    return
  }
  if (typeof schema.$ref === 'string') {
    const reference = schema.$ref as string
    const key = reference.match(/^#\/\$defs\/([A-Za-z0-9_-]+)$/)?.[1]
    const target = key && (root.$defs as Json | undefined)?.[key]
    if (!target || typeof target !== 'object') fail(`schema_ref:${where}`)
    return validateAgainstSchema(value, target as Json, where, root)
  }
  if (schema.const !== undefined && value !== schema.const) fail(`schema_const:${where}`)
  if (schema.enum && !(schema.enum as unknown[]).includes(value)) fail(`schema_enum:${where}`)
  if (schema.type === 'object') { if (!value || typeof value !== 'object' || Array.isArray(value)) fail(`schema_type:${where}`); const object = value as Json; for (const key of (schema.required as string[] ?? [])) if (!(key in object)) fail(`schema_required:${where}.${key}`); if (schema.additionalProperties === false) for (const key of Object.keys(object)) if (!(key in (schema.properties as Json))) fail(`schema_unknown:${where}.${key}`); for (const [key, child] of Object.entries(schema.properties as Json ?? {})) if (key in object) validateAgainstSchema(object[key], child as Json, `${where}.${key}`, root) }
  if (schema.type === 'array') { if (!Array.isArray(value)) fail(`schema_type:${where}`); const items = value as unknown[]; const minItems = typeof schema.minItems === 'number' ? schema.minItems : undefined; const maxItems = typeof schema.maxItems === 'number' ? schema.maxItems : undefined; if (minItems !== undefined && items.length < minItems) fail(`schema_min_items:${where}`); if (maxItems !== undefined && items.length > maxItems) fail(`schema_max_items:${where}`); if (schema.items) items.forEach((entry: unknown, index: number) => validateAgainstSchema(entry, schema.items as Json, `${where}[${index}]`, root)) }
  if (schema.type === 'string') { if (typeof value !== 'string') fail(`schema_type:${where}`); const text = value as string; if (typeof schema.minLength === 'number' && text.length < schema.minLength) fail(`schema_min_length:${where}`); if (typeof schema.pattern === 'string' && !(new RegExp(schema.pattern).test(text))) fail(`schema_pattern:${where}`) }
  if (schema.type === 'boolean' && typeof value !== 'boolean') fail(`schema_type:${where}`)
  if (schema.type === 'number' && typeof value !== 'number') fail(`schema_type:${where}`)
}
const validateOperational = (value: unknown, definition: string, kind: string): void => {
  const document = JSON.parse(readFileSync(paths.schema, 'utf8')) as Json
  try { validateAgainstSchema(value, { $ref: `#/$defs/${definition}` }, '$', document) } catch { fail(`invalid_${kind}_schema`) }
}

const generate = () => {
  const data = manifest(); validateManifest(data)
  const gateCommand = (task: any, phase: string) => {
    if (task.id === 'PLAN-001' && phase === 'author_direct') return 'node --import tsx --test scripts/readiness-plan.test.ts'
    const command = /^ENG-|^DST-|^DSR-|^STO-/.test(task.id) ? 'pnpm engine:test' : /^E2E-|^TST-/.test(task.id) ? 'pnpm test' : /^PG18-|^PGR-/.test(task.id) ? 'pnpm typecheck' : /^SWF-|^CMP-|^APP-/.test(task.id) ? 'pnpm test' : 'pnpm typecheck'
    return `${command} # readiness ${task.id} ${phase}`
  }
  const closed = (required: string[], properties: Json) => ({ type: 'object', additionalProperties: false, required, properties })
  const ref = (name: string) => ({ $ref: `#/$defs/${name}` })
  const defs: Json = {
    identifier: { type: 'string', minLength: 1, pattern: '^[A-Za-z0-9._:/-]+$' },
    hash: { type: 'string', pattern: '^[a-f0-9]{40,64}$' },
    string_list: { type: 'array', items: { type: 'string', minLength: 1 } },
    profile: closed(['lane','features'], { lane: { enum: ['COMPAT_V1','NATIVE_CORE'] }, features: ref('string_list') }),
    execution_scope: closed(['kind'], { kind: { enum: ['shared_producer','per_profile'] }, id: ref('identifier'), profile_scope: ref('identifier') }),
    conditional: closed(['when','requires'], { when: { type: 'string', pattern: '^(true|false|[CNAUST!&() ]+)$' }, requires: ref('string_list') }),
    integrated_edge: ref('identifier'),
    red_artifact_edge: closed(['requires','type','provider','consumer','scenario_registry_requirement','scope','base','independent_review'], { requires: ref('identifier'), type: { const: 'red_artifact' }, provider: ref('identifier'), consumer: ref('identifier'), scenario_registry_requirement: { const: 'registered_current_base_reviewed' }, scope: { const: 'profile' }, base: { const: 'current_integration_base' }, independent_review: { type: 'boolean' } }),
    red_artifact: closed(['identity','provider_task','consumer_task','scenario_id','profile_scope','semantic_hash','base_sha','red_patch_sha','red_tree_sha','red_evidence_sha','author_id','reviewer_id','review_state'], { identity: ref('hash'), provider_task: ref('identifier'), consumer_task: ref('identifier'), scenario_id: ref('identifier'), profile_scope: ref('identifier'), semantic_hash: ref('hash'), base_sha: ref('hash'), red_patch_sha: ref('hash'), red_tree_sha: ref('hash'), red_evidence_sha: ref('hash'), author_id: ref('identifier'), reviewer_id: ref('identifier'), review_state: { const: 'red_proved' } }),
    dependencies: closed(['integrated','conditional','red_artifacts'], { integrated: { type: 'array', items: ref('integrated_edge') }, conditional: { type: 'array', items: ref('conditional') }, red_artifacts: { type: 'array', items: ref('red_artifact_edge') } }),
    resources: closed(['read','write','semantic','runtime'], { read: ref('string_list'), write: ref('string_list'), semantic: ref('string_list'), runtime: ref('string_list') }),
    task: closed(['id','title','dependencies','applicability','execution_scope','proof_kind','owner','principal_write_boundary','artifacts','read_resources','write_resources','semantic_resources','runtime_resources','scenario_ids','acceptance','resources'], { id: ref('identifier'), title: { type: 'string', minLength: 1 }, dependencies: ref('dependencies'), applicability: { type: 'string', pattern: '^(true|false|[CNAUST!&() ]+)$' }, execution_scope: { enum: ['shared_producer','per_profile'] }, proof_kind: { enum: ['non_behavioral','genuine_red','inherited_control'] }, owner: { type: 'string', minLength: 1 }, principal_write_boundary: { type: 'string', minLength: 1 }, artifacts: ref('string_list'), read_resources: ref('string_list'), write_resources: ref('string_list'), semantic_resources: ref('string_list'), runtime_resources: ref('string_list'), scenario_ids: ref('string_list'), acceptance: { type: 'string' }, resources: ref('resources') }),
    authority: closed(['path','blob_sha'], { path: { type: 'string', minLength: 1 }, blob_sha: ref('hash') }),
    unavailable_registry: closed(['kind','owner'], { kind: { const: 'unavailable' }, owner: ref('identifier') }),
    evidence: closed(['source_strategy','source_commit','source_tree','pre_attestation_sha256','post_attestation_sha256','external_input_manifest_sha256','mount_topology_sha256','run_root_identity','empty_run_root_attestation_sha256','effective_config_sha256','source_clean','mount_read_only','run_root_new_empty','post_source_unchanged','external_inputs_unchanged','config_matches'], { source_strategy: { enum: ['fresh_detached_worktree','verified_tree_export','author_checkout','control_checkout'] }, source_commit: ref('hash'), source_tree: ref('hash'), pre_attestation_sha256: ref('hash'), post_attestation_sha256: ref('hash'), external_input_manifest_sha256: ref('hash'), mount_topology_sha256: ref('hash'), run_root_identity: ref('identifier'), empty_run_root_attestation_sha256: ref('hash'), effective_config_sha256: ref('hash'), source_clean: { type: 'boolean' }, mount_read_only: { type: 'boolean' }, run_root_new_empty: { type: 'boolean' }, post_source_unchanged: { type: 'boolean' }, external_inputs_unchanged: { type: 'boolean' }, config_matches: { type: 'boolean' }, overlay_writable: { type: 'boolean' }, run_root_reused: { type: 'boolean' }, source_dirty: { type: 'boolean' }, post_source_mutated: { type: 'boolean' }, config_mismatch: { type: 'boolean' }, external_input_mutated: { type: 'boolean' } }),
    baseline_assertion: closed(['owner','assertion'], { owner: { type: 'string', minLength: 1 }, assertion: { type: 'string', minLength: 1 } }),
    gate: closed(['id','task_id','phase','applicability','owner','command','config_identity','evidence_fields','baseline_assertion'], { id: ref('identifier'), task_id: ref('identifier'), phase: { enum: ['author_direct','merge_direct','final_release_qualification'] }, applicability: { type: 'string', minLength: 1 }, owner: { type: 'string', minLength: 1 }, command: { type: 'string', minLength: 1 }, config_identity: ref('identifier'), evidence_fields: ref('string_list'), baseline_assertion: { anyOf: [{ const: null }, ref('baseline_assertion')] } }),
    packet_base: closed(['initial_head','integration_tree'], { initial_head: ref('hash'), integration_tree: ref('hash') }),
    packet_topology: closed(['proof_kind','red_artifact_input'], { proof_kind: { enum: ['non_behavioral','genuine_red','inherited_control'] }, red_artifact_input: { anyOf: [{ const: null }, ref('red_artifact')] } }),
    packet_contract: closed(['scenario_registry_identity','scenario_registry'], { scenario_registry_identity: { anyOf: [{ const: null }, ref('hash')] }, scenario_registry: { enum: ['not_applicable_pre_registry','registered'] } }),
    output_identity: closed(['name','path','git_blob_sha','sha256'], { name: ref('identifier'), path: { type: 'string', minLength: 1 }, git_blob_sha: ref('hash'), sha256: ref('hash') }),
    bootstrap_packet: closed(['packet_kind','task_id','profile','release_profile_hash','execution_scope','base','control'], { packet_kind: { const: 'bootstrap_plan' }, task_id: { const: 'PLAN-001' }, profile: { const: null }, release_profile_hash: { const: null }, execution_scope: ref('execution_scope'), base: ref('packet_base'), control: ref('lease') }),
    ordinary_packet: closed(['packet_version','packet_kind','task_id','attempt','execution_scope','profile','predecessors','topology','contract','control','base','output_identities','output_bundle_sha256','authority','core_sha256'], { packet_version: { const: 2 }, packet_kind: { const: 'task' }, task_id: ref('identifier'), attempt: { type: 'number' }, execution_scope: ref('execution_scope'), profile: { const: null }, predecessors: ref('string_list'), topology: ref('packet_topology'), contract: ref('packet_contract'), control: ref('lease'), base: ref('packet_base'), output_identities: { type: 'array', minItems: 6, maxItems: 6, items: ref('output_identity') }, output_bundle_sha256: ref('hash'), authority: closed(['integration_commit','integration_tree','canonical_inputs'], { integration_commit: ref('hash'), integration_tree: ref('hash'), canonical_inputs: { type: 'array', minItems: 2, maxItems: 2, items: ref('authority') } }), core_sha256: ref('hash') }),
    scenario: closed(['scenario_id','semantic_hash','owner','profile_expression','oracle_hash','exclusions_hash','evidence_schema_hash'], { scenario_id: ref('identifier'), semantic_hash: ref('hash'), owner: ref('identifier'), profile_expression: { type: 'string', minLength: 1 }, oracle_hash: ref('hash'), exclusions_hash: ref('hash'), evidence_schema_hash: ref('hash') }),
    scenario_registry: closed(['kind','identity','scenarios'], { kind: { const: 'registered' }, identity: ref('hash'), scenarios: { type: 'array', items: ref('scenario') } }),
    lease: closed(['heartbeat_auth_ref','reservation_lease_id','scheduler_generation','authenticated','generation','current_generation','packet_sha256','heartbeat_nonce','acknowledged_nonce','ttl_ms','heartbeat_interval_ms','expires_at','last_heartbeat_at','heartbeat_deadline_ms','control_plane_available'], { heartbeat_auth_ref: ref('identifier'), reservation_lease_id: ref('identifier'), scheduler_generation: { type: 'number' }, authenticated: { type: 'boolean' }, generation: { type: 'number' }, current_generation: { type: 'number' }, packet_sha256: ref('hash'), heartbeat_nonce: { type: 'number' }, acknowledged_nonce: { type: 'number' }, ttl_ms: { type: 'number' }, heartbeat_interval_ms: { type: 'number' }, expires_at: { type: 'number' }, last_heartbeat_at: { type: 'number' }, heartbeat_deadline_ms: { type: 'number' }, control_plane_available: { type: 'boolean' } }),
    resolution: closed(['task_id','scope_id','outcome','state','generation','base'], { task_id: ref('identifier'), scope_id: ref('identifier'), outcome: { enum: ['pass','fail','blocked','not_applicable_by_profile'] }, state: { enum: ['integrated','qualified','rejected','invalidated'] }, generation: { type: 'number' }, base: closed(['head','tree'], { head: ref('hash'), tree: ref('hash') }) }),
    controller_state: closed(['generation','integration_head','integration_tree','resolutions','lease'], { generation: { type: 'number' }, integration_head: ref('hash'), integration_tree: ref('hash'), resolutions: { type: 'array', items: ref('resolution') }, lease: ref('lease') }),
    outcome: closed(['task_id','profile','outcome','state'], { task_id: ref('identifier'), profile: ref('profile'), outcome: { enum: ['pass','fail','blocked','not_applicable_by_profile'] }, state: { enum: ['declared','ready','reserved','red_proved','characterized','implemented','reviewed','integrated','qualified','rejected','invalidated'] } }),
  }
  const schema: Json = { ...closed(['version','canonicalization','authoritative_inputs','future_inputs','tasks','conditional_edges'], { version: { const: 1 }, canonicalization: { const: 'json-sort-keys-v1' }, authoritative_inputs: { type: 'array', minItems: 2, maxItems: 2, items: ref('authority') }, future_inputs: closed(['release_profiles','scenario_registry'], { release_profiles: ref('unavailable_registry'), scenario_registry: ref('unavailable_registry') }), tasks: { type: 'array', minItems: 169, maxItems: 169, items: ref('task') }, conditional_edges: { type: 'array', minItems: 36, maxItems: 36, items: closed(['consumer','when','requires'], { consumer: ref('identifier'), when: { type: 'string', pattern: '^(true|false|[CNAUST!&() ]+)$' }, requires: ref('string_list') }) } }), $defs: defs }
  const gates = { version: 1, evidence_required: ['source_strategy','source_commit','source_tree','pre_attestation_sha256','post_attestation_sha256','external_input_manifest_sha256','mount_topology_sha256','run_root_identity','empty_run_root_attestation_sha256','effective_config_sha256'], evidence_requirements: { source_strategy: 'fresh_detached_worktree|verified_tree_export', pre_post: true, immutable_mount: true, initially_empty_run_root: true, effective_config: true }, gates: data.tasks.flatMap(task => ['author_direct','merge_direct','final_release_qualification'].map(phase => ({ id: `${task.id}:${phase}`, task_id: task.id, phase, applicability: task.applicability, owner: task.owner, command: gateCommand(task, phase), config_identity: `task:${task.id}:canonical-effective-config`, evidence_fields: ['source_commit','source_tree','pre_attestation_sha256','post_attestation_sha256','external_input_manifest_sha256','mount_topology_sha256','run_root_identity','empty_run_root_attestation_sha256','effective_config_sha256'], baseline_assertion: task.id === 'TST-000' ? { owner: task.owner, assertion: 'baseline-inventory-only' } : null }))) }
  writeFileSync(paths.tasks, `${JSON.stringify(data, null, 2)}\n`); writeFileSync(paths.schema, `${JSON.stringify(schema, null, 2)}\n`); writeFileSync(paths.gates, `${JSON.stringify(gates, null, 2)}\n`)
  const identityProjection = outputIdentities(false).outputs.map(identity => `- ${identity.path}: git blob ${identity.git_blob_sha}; SHA-256 ${identity.sha256}`)
  const report = ['# Generated readiness plan','','- Authoritative task inventory: 169','- Normative conditional edges: 36','- Future release profiles: unavailable until GOV-005','- Future scenario registry: unavailable until E2E-000S','','## First ready sets','','- `shared-pre-registry` after `PLAN-001`: `GOV-001`, `TST-000`','- `shared-pre-registry` after `PLAN-001,GOV-001`: `CMP-000`, `GOV-002`, `GOV-003`, `SEC-008B`, `TST-000`','','## Ownership and gate matrix','','- 169 principal boundaries, 507 phase rows, and one owner/applicability binding per row.','- Direct, merge, and qualification commands are chosen by task capability family; PLAN-001 author direct runs its contract suite.','','## Deterministic identities','','The non-self-referential projection below binds the five peer outputs. The `identity` command emits all six current output identities, including this report, for packet handoff.','', ...identityProjection, '', 'Use `pnpm exec tsx scripts/readiness-plan.ts identity` to emit the complete six-output identity set.'].join('\n')
  writeFileSync(paths.report, `${report}\n`)
}
const main = () => { const [command, ...args] = process.argv.slice(2); if (command === 'generate') return generate(); const data = JSON.parse(readFileSync(paths.tasks, 'utf8')) as ReturnType<typeof manifest>; if (command === 'validate') { const bootstrapPath = args[args.indexOf('--bootstrap-packet') + 1]; if (bootstrapPath) { const packet = JSON.parse(readFileSync(resolve(root, bootstrapPath), 'utf8')); validatePacket(packet, data); return } validateAgainstSchema(data, JSON.parse(readFileSync(paths.schema, 'utf8'))); validateManifest(data); return validateGates(data, JSON.parse(readFileSync(paths.gates, 'utf8'))) } if (command === 'identity') return console.log(JSON.stringify(outputIdentities(), null, 2)); if (command === 'packet') { const statePath = args[args.indexOf('--state') + 1], controlPath = args[args.indexOf('--control') + 1]; if (!statePath) fail('controller_state_required'); if (!controlPath) fail('controller_lease_required'); return console.log(JSON.stringify(buildPlanningPacket(data, args[args.indexOf('--task') + 1], JSON.parse(readFileSync(resolve(root, statePath), 'utf8')), JSON.parse(readFileSync(resolve(root, controlPath), 'utf8'))), null, 2)) } if (command === 'ready') { const scope = args[args.indexOf('--scope') + 1]; const statePath = args[args.indexOf('--state') + 1]; if (!statePath) fail('controller_state_required'); const controller = JSON.parse(readFileSync(resolve(root, statePath), 'utf8')); const completed = new Set((controller.resolutions as Json[] ?? []).filter(row => row.outcome === 'pass' && row.state === 'integrated').map(row => row.task_id as string)); console.log(JSON.stringify(ready(data, null, completed, scope))); return } fail('unknown_command') }
if (process.argv[1] === new URL(import.meta.url).pathname) main()
