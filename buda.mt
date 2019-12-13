import "lib/codec/utf8" =~ [=> UTF8]
import "lib/streams" =~ [=> Sink, => makeSink, => flow]
exports (makeBuda, rulesFromMap, which)
def partialFlow(source, sink) :Vow[Void] as DeepFrozen:
    "Flow all packets from `source` to `sink`, like flow(), but don't tell the sink we're done."

    if (Ref.isBroken(sink)):
        return sink
    if (Ref.isBroken(source)):
        return source

    def [p, r] := Ref.promise()
    object flowSink as Sink:
        to run(packet) :Vow[Void]:
            return when (sink<-(packet)) ->
                source<-(flowSink)
                null
            catch problem:
                r.smash(problem)
                sink.abort(problem)
                Ref.broken(problem)

        to complete() :Vow[Void]:
            r.resolve(null)

        to abort(problem) :Vow[Void]:
            r.smash(problem)
            return sink.abort(problem)

    return when (source<-(flowSink)) ->
        p
    catch problem:
        r.smash(problem)
        null

def which(makeFileResource, pathString :Bytes, target :Bytes, "FAIL" => ej) as DeepFrozen:
    "Unix which. Find a file by name in $PATH."
    def paths := pathString.split(b`:`).reverse().diverge()
    def check():
        if (paths.size() == 0):
            throw.eject(ej, `$target not found`)
        var loc := paths.pop()
        if (loc.slice(loc.size() - 1) != b`/`):
            loc += b`/`
        loc += target
        def fp := makeFileResource(UTF8.decode(loc, null)).getContents()
        return when (fp) ->
            loc
        catch _:
            check()
    return check()

def rulesFromMap(rulesMap :Map[Bytes, Any], defaults :Map[Bytes, Any]) as DeepFrozen:
    return def findRule(target :Bytes, "FAIL" => ej := null):
        def rule := rulesMap.fetch(target, fn {})
        if (rule == null):
            for suffix => rule in (defaults):
                # no endsWith on bytes
                if (target.slice(target.size() - suffix.size()) == suffix):
                    return [rule, target.slice(0, target.size() - suffix.size())]
            throw.eject(ej, `No rule found for ${UTF8.decode(target, null)}`)
        else:
            return [rule, target]

def makeBuda(findRule, => makeProcess, => makeFileResource, => stdio) as DeepFrozen:
    def artifactMap := [].asMap().diverge()
    def dependencyMap := [].asMap().diverge()
    def depsCache := [].asMap().diverge()
    def stdout := stdio.stdout()
    def storeResult(buildResult :Bytes) :Vow[Bytes]:
        def [hash, sink] := makeSink.asBytes()
        def proc := makeProcess(b`git`, [b`git`, b`hash-object`, b`-w`, b`--stdin`], [].asMap(), "stdout" => true, "stderr" => true, "stdin" => true)
        traceln(`loading ${buildResult.size()} bytes`)
        proc.stdin()(buildResult)
        proc.stdin().complete()
        partialFlow(proc.stderr(), stdout)
        flow(proc.stdout(), sink)
        return hash <- trim()

    def writeArtifactMap(artifacts :Map[Bytes, Bytes]):
        def [hash, sink] := makeSink.asBytes()
        def proc := makeProcess(b`git`, [b`git`, b`mktree`, b`-z`], [].asMap(), "stdout" => true, "stderr" => true, "stdin" => true)
        traceln(`writing artifact map`)
        def stdin := proc.stdin()
        for name => hash in (artifacts):
            stdin(b`100644 blob `)
            stdin(hash)
            stdin(b`$\x09`)
            for depName in (dependencyMap[name]):
                stdin(artifacts[depName])
                stdin(b`-`)
            stdin(name)
            stdin(b`$\x00`)
        proc.stdin().complete()
        partialFlow(proc.stderr(), stdout)
        flow(proc.stdout(), sink)
        return hash <- trim()

    def storeArtifact(tmpTarget :Bytes, target :Bytes):
        return when (def f := makeFileResource(UTF8.decode(tmpTarget, null)).getContents()) ->
            when (def hash := storeResult(f)) ->
                artifactMap[target] := hash
        catch q:
            traceln(`did not write to $target`)
            traceln.exception(q)
            q

    return object buda:
        to main(target):

            return when (buda <- maybeLoadCache() <- (target)) ->
                when (def ahash := writeArtifactMap(artifactMap.snapshot())) ->
                    when (buda.do(b`git`, [b`git`, b`update-ref`, b`refs/buda/latest`, ahash])) ->
                        traceln(`Done.$\n${ahash}`)
                        0
            catch p:
                traceln.exception(p)
                1
            catch ee:
                traceln.exception(ee)
                1

        to maybeLoadCache() :
            def [treeDesc, sink] := makeSink.asBytes()
            def proc := makeProcess(b`git`, [b`git`, b`ls-tree`, b`-z`, b`refs/buda/latest`], [].asMap(), "stdout" => true, "stderr" => true)
            traceln(`loading artifact cache`)
            return when (def ex := proc.wait()) ->
                if (ex.exitStatus() == 0):
                    flow(proc.stdout(), sink)
                    # XXX state machine for parsing instead of waiting
                    when (treeDesc) ->
                        def lines := treeDesc.split(b`$\x00`)
                        for line in (lines):
                            if (line.size() == 0):
                                continue
                            def [data, filename] := line.split(b`$\t`, 1)
                            def [_perms, _kind, hash] := data.split(b` `)
                            def pieces := filename.split(b`-`).diverge()
                            def name := pieces.pop()
                            depsCache[name] := pieces.snapshot()
                            artifactMap[name] := hash
                        buda
                else:
                    flow(proc.stderr(), sink)
                    when (treeDesc) -> { traceln(`Loading cache failed: ${treeDesc}`) }
                    buda

        to fulfillDependencies(target, rule, prefix):
            def deps :List[Bytes] := rule.getDependencies()
            # Fetch old hashes, run all deps collecting new hashes, compare. If different, run build rule. Otherwise just keep old hash for this target
            dependencyMap[target] := []
            def dependencyOutputs := [].diverge()
            traceln(`rule $rule deps $deps`)
            def queue := deps.diverge()
            # process deps serially for now, in theory we could do this in parallel
            def fulfillDep():
                def dep := queue.pop()
                def depName:= if (dep[0] == b`*`[0]) {
                    prefix + dep.slice(1)
                } else {
                    dep
                }
                dependencyMap[target] with= (depName)
                return when (def hash := buda(depName)) ->
                    dependencyOutputs.push(hash)
                    if (queue.size() > 0):
                        fulfillDep()
            return when (fulfillDep()) ->
                dependencyOutputs.snapshot() != depsCache[target]
        to run(target):
            escape e:
                def [rule, prefix] := findRule(target, "FAIL" => e)
                traceln(`target $target rule $rule prefix $prefix`)
                return when (def dirty := buda.fulfillDependencies(target, rule, prefix)) ->
                    if (dirty):
                        def tmpTarget := target + b`.buda.tmp`
                        traceln(`starting $target`)
                        when (rule(buda, prefix, tmpTarget)) ->
                            traceln(`finished $target`)
                            when (def hash := storeArtifact(tmpTarget, target)) ->
                                buda.do(b`mv`, [b`mv`, tmpTarget, target])
                                hash
                    else:
                        traceln(`$target is clean`)
                        artifactMap[target]
            catch p:
                # no rule means unconditionally store contents
                return when (buda.do(b`test`, [b`test`, b`-e`, target])) ->
                    dependencyMap[target] := []
                    storeArtifact(target, target)
                catch _:
                    p

        to do(path :Bytes, argv :List[Bytes]):
            "Run a subprocess, collect stdout and stderr if it fails."
            traceln(`do: ${" ".join([for via (UTF8.decode) arg in (argv) arg])}`)
            def proc := makeProcess(path, argv, [].asMap(), "stdout" => true,
                                    "stderr" => true)
            def exitInfo := proc.wait()
            return when (exitInfo) ->
                def code := exitInfo.exitStatus()
                if (code != 0):
                    partialFlow(proc.stdout(), stdout)
                    partialFlow(proc.stderr(), stdout)
                    Ref.broken(`Failure running ``${" ".join([for via (UTF8.decode) s in (argv) s])}``:Error code ${code}`)
            catch p:
                traceln.exception(p)
                p
