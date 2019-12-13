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

    return object buda:
        to main(target):
            return when (buda(target)) ->
                traceln(`Done.$\n${artifactMap}`)
                0
            catch p:
                traceln.exception(p)
                1
            catch ee:
                traceln.exception(ee)
                1

        to fulfillDependencies(rule, prefix):
            def deps :List[Bytes] := rule.getDependencies()
            traceln(`rule $rule deps $deps`)
            def queue := deps.diverge()
            # process deps serially for now, in theory we could do this in parallel
            def fulfillDep():
                def dep := queue.pop()
                traceln(`dep $dep`)
                def p := if (dep[0] == b`*`[0]) {
                    buda(prefix + dep.slice(1))
                } else {
                    buda(dep)
                }
                return when (p) ->
                    if (queue.size() > 0):
                        fulfillDep()
            return fulfillDep()
        to run(target):
            escape e:
                def [rule, prefix] := findRule(target, "FAIL" => e)
                traceln(`target $target rule $rule prefix $prefix`)
                return when (buda.fulfillDependencies(rule, prefix)) ->
                    def tmpTarget := target + b`.buda.tmp`
                    traceln(`starting $target`)
                    when (rule(buda, prefix, tmpTarget)) ->
                        traceln(`finished $target`)
                        when (def f := makeFileResource(UTF8.decode(tmpTarget, null)).getContents()) ->
                            when (def hash := storeResult(f)) ->
                                artifactMap[target] := hash
                                buda.do(b`mv`, [b`mv`, tmpTarget, target])
                            catch p:
                                traceln.exception(p)
                                p
                        catch q:
                            traceln(`$rule did not write to $tmpTarget`)
                            traceln.exception(q)
                            q

            catch p:
                # couldn't find a rule to rebuild a target, but maybe it exists?
                return when (buda.do(b`test`, [b`test`, b`-e`, target])) ->
                    null
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
