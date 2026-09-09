using Test

function physical_metadata()
    DBN.Metadata(DBN.DBN_VERSION, "TEST.MOCK", DBN.Schema.CMBP_1, Int64(0), nothing,
        nothing, nothing, DBN.SType.INSTRUMENT_ID, false, ["SPY"], String[], String[],
        Tuple{String,String,Int64,Int64}[])
end
function physical_cmbp(sequence::UInt32; action=DBN.Action.ADD, side=DBN.Side.BID)
    timestamp = Int64(1_800_000_000_000_000_000 + sequence)
    header = DBN.RecordHeader(UInt8(20), DBN.RType.CMBP_1_MSG, UInt16(17), UInt32(101), timestamp)
    level = DBN.BidAskPair(Int64(500_000_000_000 + sequence), Int64(500_010_000_000 + sequence),
        UInt32(10 + sequence), UInt32(20 + sequence), UInt32(2), UInt32(3))
    DBN.CMBP1Msg(header, Int64(500_005_000_000 + sequence), UInt32(7 + sequence),
        action, side, UInt8(0xa5), UInt8(0), timestamp + 10, Int32(-7), sequence, level)
end
function write_physical(path, records)
    DBN.write_dbn(path, physical_metadata(), DBN.DBNRecord[records...]); path
end
function physical_caps(path; records=16, metadata=1_000_000, logical=1_000_000, physical=filesize(path) + 4)
    DBN.PhysicalStreamCapsV1(max_metadata_bytes=metadata, max_logical_bytes=logical,
        max_physical_source_bytes=physical, max_records=records)
end
function first_record_offset(bytes)
    9 + Int(ltoh(read(IOBuffer(bytes[5:8]), UInt32)))
end

@testset "bounded physical CMBP-1 streaming" begin
    mktempdir() do directory
        rows = [physical_cmbp(UInt32(11)),
            physical_cmbp(UInt32(12); action=DBN.Action.CANCEL, side=DBN.Side.ASK)]
        for path in (write_physical(joinpath(directory, "physical.dbn"), rows),
                     write_physical(joinpath(directory, "physical.dbn.zst"), rows))
            events = DBN.PhysicalRecordEventV1[]
            summary = DBN.foreach_physical_cmbp1(path, physical_caps(path)) do event
                push!(events, event); DBN.PHYSICAL_CONTINUE
            end
            @test summary.source_closed && summary.eof_reached
            @test summary.terminal_reason == :eof
            @test summary.compressed == endswith(path, ".zst")
            @test summary.records_read == 2 && summary.disposition_counts[:cmbp1] == 2
            @test summary.counters.logical_bytes_consumed > 0
            @test summary.counters.physical_source_bytes_pulled > 0
            @test summary.counters.decompressed_read_ahead_bytes >= 0
            @test getfield.(events, :ordinal) == UInt64[0, 1]
            @test all(e -> e.disposition == :cmbp1, events)
            @test events[1].raw_action_byte == UInt8(DBN.Action.ADD)
            @test events[1].raw_side_byte == UInt8(DBN.Side.BID)
            @test events[2].raw_action_byte == UInt8(DBN.Action.CANCEL)
            @test events[2].raw_side_byte == UInt8(DBN.Side.ASK)
            @test events[1].header.publisher_id == 17 && events[1].header.instrument_id == 101
            @test events[1].record.sequence == 11 && events[1].record.flags == 0xa5
            @test events[1].record.levels.bid_px == 500_000_000_011
            @test events[2].record.levels.ask_sz == 32
        end
        stopped = DBN.foreach_physical_cmbp1(_ -> DBN.PHYSICAL_STOP,
            joinpath(directory, "physical.dbn"), physical_caps(joinpath(directory, "physical.dbn")))
        @test stopped.source_closed && stopped.terminal_reason == :callback_stop && stopped.records_read == 1
        retained = DBN.foreach_physical_cmbp1(joinpath(directory, "physical.dbn"),
            physical_caps(joinpath(directory, "physical.dbn"))) do event
            event.ordinal == 1 ? DBN.PHYSICAL_RETAIN_LOOKAHEAD : DBN.PHYSICAL_CONTINUE
        end
        @test retained.source_closed && retained.terminal_reason == :retained_lookahead
        @test retained.retained_lookahead.ordinal == 1
    end
end

@testset "physical dispositions and malformed terminalization" begin
    mktempdir() do directory
        control = DBN.SystemMsg(DBN.RecordHeader(UInt8(18), DBN.RType.SYSTEM_MSG,
            UInt16(0), UInt32(0), Int64(7)), "heartbeat", "")
        path = write_physical(joinpath(directory, "control.dbn"), [control, physical_cmbp(UInt32(1))])
        dispositions = Symbol[]
        summary = DBN.foreach_physical_cmbp1(path, physical_caps(path)) do event
            push!(dispositions, event.disposition); DBN.PHYSICAL_CONTINUE
        end
        @test dispositions == [:control, :cmbp1] && summary.disposition_counts[:control] == 1

        bytes = read(path); offset = first_record_offset(bytes); bytes[offset + 1] = 0xfe
        unknown = joinpath(directory, "unknown.dbn"); write(unknown, bytes)
        events = DBN.PhysicalRecordEventV1[]
        summary = DBN.foreach_physical_cmbp1(unknown, physical_caps(unknown)) do event
            push!(events, event); DBN.PHYSICAL_CONTINUE
        end
        @test getfield.(events, :disposition) == [:unknown, :cmbp1]
        @test events[1].header.rtype_byte == 0xfe && summary.disposition_counts[:unknown] == 1

        bad = write_physical(joinpath(directory, "bad.dbn"), [physical_cmbp(UInt32(2))])
        bytes = read(bad); bytes[first_record_offset(bytes)] = UInt8(19); write(bad, bytes)
        bad_events = DBN.PhysicalRecordEventV1[]
        bad_summary = DBN.foreach_physical_cmbp1(bad, physical_caps(bad)) do event
            push!(bad_events, event); DBN.PHYSICAL_CONTINUE
        end
        @test only(bad_events).reason == :unexpected_cmbp1_length
        @test bad_summary.terminal_reason == :malformed_record && bad_summary.source_closed

        truncated = write_physical(joinpath(directory, "truncated.dbn"), [physical_cmbp(UInt32(3))])
        bytes = read(truncated); resize!(bytes, length(bytes) - 5); write(truncated, bytes)
        truncated_events = DBN.PhysicalRecordEventV1[]
        truncated_summary = DBN.foreach_physical_cmbp1(truncated, physical_caps(truncated)) do event
            push!(truncated_events, event); DBN.PHYSICAL_CONTINUE
        end
        @test only(truncated_events).reason == :truncated_body
        @test truncated_summary.terminal_reason == :malformed_record && truncated_summary.source_closed
    end
end

@testset "physical caps and exceptional cleanup" begin
    mktempdir() do directory
        rows = [physical_cmbp(UInt32(1)), physical_cmbp(UInt32(2))]
        plain = write_physical(joinpath(directory, "caps.dbn"), rows)
        zstd = write_physical(joinpath(directory, "caps.dbn.zst"), rows)
        cases = [(:metadata_bytes, plain, physical_caps(plain; metadata=8)),
            (:logical_bytes, plain, physical_caps(plain; logical=32)),
            (:records, plain, physical_caps(plain; records=1)),
            (:physical_source_bytes, zstd, physical_caps(zstd; physical=16))]
        for (bound, path, caps) in cases
            caught = try
                DBN.foreach_physical_cmbp1(_ -> DBN.PHYSICAL_CONTINUE, path, caps); nothing
            catch error
                error
            end
            @test caught isa DBN.PhysicalStreamLimitError
            @test caught.bound == bound && caught.observed > caught.limit
            @test caught.source_closed
        end
        @test_throws ErrorException DBN.foreach_physical_cmbp1(plain, physical_caps(plain)) do _
            error("callback failure")
        end
        @test open(isopen, plain)
    end
end
