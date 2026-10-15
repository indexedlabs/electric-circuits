import os, pathlib, subprocess, time, signal, shutil, json
root=pathlib.Path.cwd(); lab=root/'.agent/lab-build'
env={'PATH':'/Users/mh/.rustup/toolchains/1.96.0-aarch64-apple-darwin/bin:/opt/homebrew/bin:/usr/bin:/bin','HOME':str(lab/'home'),'CARGO_HOME':str(lab/'cargo-home'),'TMPDIR':str(lab/'tmp'),'CARGO_TARGET_DIR':str(lab/'target'),'CARGO_BUILD_JOBS':'6','CARGO_PROFILE_DEV_DEBUG':'0','CARGO_PROFILE_TEST_DEBUG':'0','CARGO_INCREMENTAL':'0','CARGO_NET_OFFLINE':'true'}
(lab/'env.json').write_text(json.dumps(env,indent=2))
cmd=['cargo','build','--locked','--offline','-p','electric-circuits-engine','--features','test-support']
with (root/'.agent/results/build.log').open('w') as log:
 p=subprocess.Popen(cmd,cwd=lab/'home',env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 (root/'.agent/results/build-pid.json').write_text(json.dumps({'pid':p.pid,'command':cmd,'source':'224cd1c'}))
 print('build pid',p.pid,flush=True)
 while p.poll() is None:
  used=int(subprocess.run(['/usr/bin/du','-sk',str(lab)],capture_output=True).stdout.split()[0])*1024
  free=shutil.disk_usage('/').free
  if free<12_000_000_000 or used>7_500_000_000:
   print('disk guard stopping build',used,free,flush=True);os.killpg(p.pid,signal.SIGKILL);p.wait();raise SystemExit(2)
  time.sleep(3)
 print('build exit',p.returncode,flush=True)
 raise SystemExit(p.returncode)
