using Test
using CodecZstd
using SHA

function physical_metadata()
    DBN.Metadata(DBN.DBN_VERSION, "TEST.MOCK", DBN.Schema.CMBP_1, Int64(100), Int64(200),
        nothing, nothing, DBN.SType.INSTRUMENT_ID, false, ["SPY"], String[], String[],
        [("SPY", "SPY", Int64(100), Int64(200))])
end
function physical_cmbp(sequence::UInt32; action=DBN.Action.ADD, side=DBN.Side.BID,
                       bid_tail=UInt32(0x66551234), ask_tail=UInt32(0x88775678))
    timestamp = Int64(1_800_000_000_000_000_000 + sequence)
    header = DBN.RecordHeader(UInt8(20), DBN.RType.CMBP_1_MSG, UInt16(17), UInt32(101), timestamp)
    level = DBN.BidAskPair(Int64(500_000_000_000 + sequence), Int64(500_010_000_000 + sequence),
        UInt32(10 + sequence), UInt32(20 + sequence), bid_tail, ask_tail)
    DBN.CMBP1Msg(header, Int64(500_005_000_000 + sequence), UInt32(7 + sequence),
        action, side, UInt8(0xa5), UInt8(0), timestamp + 10, Int32(-7), sequence, level)
end
physical_control() = DBN.SystemMsg(DBN.RecordHeader(UInt8(18), DBN.RType.SYSTEM_MSG,
    UInt16(0), UInt32(0), Int64(7)), "heartbeat", "")
function write_physical(path, records)
    DBN.write_dbn(path, physical_metadata(), DBN.DBNRecord[records...]); path
end
function logical_fixture_bytes(path)
    bytes = read(path)
    endswith(path, ".zst") ? transcode(ZstdDecompressor, bytes) : bytes
end
function raw_metadata_bytes(path)
    bytes = logical_fixture_bytes(path)
    metadata_length = ltoh(read(IOBuffer(bytes[5:8]), UInt32))
    bytes[1:8 + Int(metadata_length)]
end
function physical_binding(path)
    raw = raw_metadata_bytes(path)
    metadata = DBN.read_header!(DBN.DBNDecoder(IOBuffer(raw))).metadata
    DBN.PhysicalMetadataBindingV1(DBN._metadata_identity(raw, metadata))
end
function physical_caps(path; records=16, metadata=1_000_000, logical=1_000_000,
                       physical=filesize(path) + 4, outer=128 * 1024, transcoder=32 * 1024)
    DBN.PhysicalStreamCapsV1(max_metadata_bytes=metadata, max_logical_bytes=logical,
        max_physical_source_bytes=physical, max_records=records,
        max_outer_buffer_capacity_bytes=outer,
        max_transcoder_buffer_capacity_bytes=transcoder)
end
first_record_offset(bytes) = 9 + Int(ltoh(read(IOBuffer(bytes[5:8]), UInt32)))

mutable struct CloseThrowIO <: IO
    io::IOStream
end
Base.isopen(io::CloseThrowIO) = isopen(io.io)
Base.eof(io::CloseThrowIO) = eof(io.io)
Base.bytesavailable(io::CloseThrowIO) = bytesavailable(io.io)
Base.position(io::CloseThrowIO) = position(io.io)
Base.seekstart(io::CloseThrowIO) = (seekstart(io.io); io)
Base.readbytes!(io::CloseThrowIO, bytes::AbstractVector{UInt8}, count::Integer=length(bytes)) =
    readbytes!(io.io, bytes, count)
Base.read(io::CloseThrowIO, ::Type{UInt8}) = read(io.io, UInt8)
function Base.close(io::CloseThrowIO)
    close(io.io)
    error("injected close failure")
end
function stream_all(path; caps=physical_caps(path), expected=physical_binding(path))
    events = DBN.PhysicalRecordEventV1[]
    summary = DBN.foreach_physical_cmbp1(path, caps, expected) do event
        push!(events, event); DBN.PHYSICAL_CONTINUE
    end
    events, summary
end

@testset "physical metadata binding and exact record identity" begin
    mktempdir() do directory
        records = [physical_control(), physical_cmbp(UInt32(11)),
            physical_cmbp(UInt32(12); action=DBN.Action.CANCEL, side=DBN.Side.ASK)]
        for path in (write_physical(joinpath(directory, "physical.dbn"), records),
                     write_physical(joinpath(directory, "physical.dbn.zst"), records))
            logical = logical_fixture_bytes(path); offset = first_record_offset(logical)
            events, summary = stream_all(path)
            @test summary.source_closed && summary.eof_reached && summary.terminal_reason == :eof
            @test summary.compressed == endswith(path, ".zst") && summary.records_read == 3
            @test getfield.(events, :ordinal) == UInt64[0, 1, 2]
            @test getfield.(events, :disposition) == [:control, :cmbp1, :cmbp1]
            @test getfield.(events, :logical_start_offset) ==
                Int64[offset - 1, offset - 1 + 72, offset - 1 + 72 + 80]
            @test getfield.(events, :logical_end_offset) ==
                Int64[offset - 1 + 72, offset - 1 + 72 + 80, offset - 1 + 72 + 160]
            for event in events
                first = event.logical_start_offset + 1
                raw = logical[first:first + event.raw_record_bytes_count - 1]
                @test event.declared_length_bytes == event.raw_record_bytes_count
                @test event.raw_record_sha256 == bytes2hex(sha256(raw))
            end
            @test events[1].header.rtype_byte == UInt8(DBN.RType.SYSTEM_MSG)
            @test events[2].raw_action_byte == UInt8(DBN.Action.ADD)
            @test events[2].raw_side_byte == UInt8(DBN.Side.BID)
            @test events[3].raw_action_byte == UInt8(DBN.Action.CANCEL)
            @test events[3].raw_side_byte == UInt8(DBN.Side.ASK)
            @test events[2].header.publisher_id == 17 && events[2].header.instrument_id == 101
            @test events[2].record.sequence == 11 && events[2].record.flags == 0xa5
            @test events[2].record.levels.bid_px == 500_000_000_011
            @test events[3].record.levels.ask_sz == 32
            for event in events[2:3]
                publisher = event.cmbp1_publisher_evidence
                @test publisher.bid_pb == 0x1234
                @test publisher.bid_reserved == (0x55, 0x66)
                @test publisher.ask_pb == 0x5678
                @test publisher.ask_reserved == (0x77, 0x88)
                @test publisher.raw_tail_bytes ==
                    (0x34, 0x12, 0x55, 0x66, 0x78, 0x56, 0x77, 0x88)
                @test event.raw_record_sha256 == bytes2hex(sha256(
                    logical[event.logical_start_offset + 1:event.logical_end_offset]))
            end
            identity = summary.metadata; raw_metadata = raw_metadata_bytes(path)
            @test identity.header_metadata_bytes == length(raw_metadata)
            @test identity.header_metadata_sha256 == bytes2hex(sha256(raw_metadata))
            @test identity.dataset == "TEST.MOCK" && identity.schema_raw == UInt16(DBN.Schema.CMBP_1)
            @test identity.stype_in_raw === nothing
            @test identity.stype_out_raw == UInt8(DBN.SType.INSTRUMENT_ID)
            @test identity.start_ts == 100 && identity.end_ts == 200
            @test identity.mapping_count == 1
            @test identity.mappings_sha256 == DBN.physical_mappings_sha256(physical_metadata().mappings)
            counters = summary.counters
            @test counters.outer_buffer_unread_bytes <= counters.outer_buffer_capacity_bytes <= 128 * 1024
            @test counters.transcoder_exposed_unread_bytes <=
                counters.transcoder_buffer_capacity_bytes <= 32 * 1024
        end
        path = joinpath(directory, "physical.dbn"); expected = physical_binding(path)
        wrong = DBN.PhysicalMetadataBindingV1(expected.header_metadata_sha256, "WRONG",
            expected.schema_raw, expected.stype_in_raw, expected.stype_out_raw, expected.start_ts,
            expected.end_ts, expected.mapping_count, expected.mappings_sha256)
        callbacks = Ref(0)
        error = try
            DBN.foreach_physical_cmbp1(path, physical_caps(path), wrong) do _
                callbacks[] += 1; DBN.PHYSICAL_CONTINUE
            end
            nothing
        catch caught; caught end
        @test error isa DBN.PhysicalStreamTerminalErrorV1
        @test error.kind == :metadata_binding && error.reason == :mismatch_dataset
        @test error.source_closed && callbacks[] == 0
        @test error.partial_bytes_sha256 == expected.header_metadata_sha256
    end
end

@testset "unknown and malformed raw identities" begin
    mktempdir() do directory
        path = write_physical(joinpath(directory, "mixed.dbn"),
            [physical_control(), physical_cmbp(UInt32(1))])
        bytes = read(path); offset = first_record_offset(bytes); bytes[offset + 1] = 0xfe
        unknown = joinpath(directory, "unknown.dbn"); write(unknown, bytes)
        events, summary = stream_all(unknown)
        @test getfield.(events, :disposition) == [:unknown, :cmbp1]
        @test getfield.(events, :ordinal) == UInt64[0, 1]
        @test events[1].header.rtype_byte == 0xfe
        @test events[1].raw_record_sha256 == bytes2hex(sha256(bytes[offset:offset + 71]))
        @test summary.disposition_counts[:unknown] == 1

        known_bytes = read(path); known_offset = first_record_offset(known_bytes)
        known_bytes[known_offset + 1] = UInt8(DBN.RType.MBP_0_MSG)
        known = joinpath(directory, "unsupported-known.dbn"); write(known, known_bytes)
        known_events, _ = stream_all(known)
        @test getfield.(known_events, :reason) == [:unsupported_known_rtype, :decoded]
        @test known_events[1].raw_record_sha256 ==
            bytes2hex(sha256(known_bytes[known_offset:known_offset + 71]))

        bad = write_physical(joinpath(directory, "bad.dbn"), [physical_cmbp(UInt32(2))])
        bytes = read(bad); offset = first_record_offset(bytes); bytes[offset] = UInt8(19); write(bad, bytes)
        bad_events, bad_summary = stream_all(bad); event = only(bad_events)
        @test event.disposition == :malformed && event.reason == :unexpected_cmbp1_length
        @test event.declared_length_bytes == 76 && event.raw_record_bytes_count == 76
        @test event.raw_record_sha256 == bytes2hex(sha256(bytes[offset:offset + 75]))
        @test bad_summary.terminal_reason == :malformed_record && bad_summary.source_closed

        invalid_length = write_physical(joinpath(directory, "invalid-length.dbn"),
            [physical_cmbp(UInt32(4))])
        bytes = read(invalid_length); offset = first_record_offset(bytes)
        bytes[offset] = UInt8(3); write(invalid_length, bytes)
        invalid_events, invalid_summary = stream_all(invalid_length)
        event = only(invalid_events)
        @test event.reason == :invalid_record_length
        @test event.declared_length_bytes == 12 && event.raw_record_bytes_count == 16
        @test event.raw_record_sha256 == bytes2hex(sha256(bytes[offset:offset + 15]))
        @test invalid_summary.source_closed

        plain = write_physical(joinpath(directory, "complete.dbn"), [physical_cmbp(UInt32(3))])
        truncated_bytes = read(plain); resize!(truncated_bytes, length(truncated_bytes) - 5)
        zstd = joinpath(directory, "truncated.dbn.zst")
        write(zstd, transcode(ZstdCompressor, truncated_bytes))
        truncated_events, truncated_summary = stream_all(zstd); event = only(truncated_events)
        offset = first_record_offset(truncated_bytes)
        @test event.disposition == :malformed && event.reason == :truncated_body
        @test event.declared_length_bytes == 80 && event.raw_record_bytes_count == 75
        @test event.raw_record_sha256 == bytes2hex(sha256(truncated_bytes[offset:end]))
        @test truncated_summary.source_closed && truncated_summary.compressed

        for compressed in (false, true)
            complete = read(plain); offset = first_record_offset(complete)
            logical = complete[1:offset + 9]
            header_path = joinpath(directory, compressed ? "header.dbn.zst" : "header.dbn")
            write(header_path, compressed ? transcode(ZstdCompressor, logical) : logical)
            header_events, header_summary = stream_all(header_path)
            event = only(header_events)
            @test event.reason == :truncated_header && event.raw_record_bytes_count == 10
            @test event.declared_length_bytes == 80
            @test event.raw_record_sha256 == bytes2hex(sha256(logical[offset:end]))
            @test header_summary.source_closed
        end
    end
end

@testset "compressed decisions and typed callback failures" begin
    mktempdir() do directory
        path = write_physical(joinpath(directory, "decisions.dbn.zst"),
            [physical_cmbp(UInt32(1)), physical_cmbp(UInt32(2))])
        stopped = DBN.foreach_physical_cmbp1(path, physical_caps(path), physical_binding(path)) do _
            DBN.PHYSICAL_STOP
        end
        @test stopped.source_closed && stopped.terminal_reason == :callback_stop
        @test stopped.records_read == 1 && !stopped.eof_reached
        retained = DBN.foreach_physical_cmbp1(path, physical_caps(path), physical_binding(path)) do event
            event.ordinal == 1 ? DBN.PHYSICAL_RETAIN_LOOKAHEAD : DBN.PHYSICAL_CONTINUE
        end
        @test retained.source_closed && retained.terminal_reason == :retained_lookahead
        @test retained.retained_lookahead.ordinal == 1
        for (kind, callback) in ((:callback, _ -> error("callback failure")),
                                 (:invalid_decision, _ -> nothing))
            caught = try
                DBN.foreach_physical_cmbp1(callback, path, physical_caps(path), physical_binding(path)); nothing
            catch error; error end
            @test caught isa DBN.PhysicalStreamTerminalErrorV1
            @test caught.kind == kind && caught.source_closed
            @test caught.ordinal == 0 && caught.partial_bytes_count == 80
            @test caught.partial_bytes_sha256 !== nothing
            @test caught.counters.logical_bytes_consumed > 0
        end
    end
end

@testset "exact caps and typed terminal failures" begin
    mktempdir() do directory
        rows = [physical_cmbp(UInt32(1)), physical_cmbp(UInt32(2))]
        plain = write_physical(joinpath(directory, "caps.dbn"), rows)
        zstd = write_physical(joinpath(directory, "caps.dbn.zst"), rows)
        logical_size = length(logical_fixture_bytes(plain))
        metadata_size = length(raw_metadata_bytes(plain)) - 8
        metadata_total = metadata_size + 8
        for path in (plain, zstd)
            exact = physical_caps(path; logical=length(logical_fixture_bytes(path)),
                metadata=length(raw_metadata_bytes(path)) - 8, records=2)
            _, summary = stream_all(path; caps=exact)
            @test summary.eof_reached && summary.source_closed
        end
        cases = [
            (:metadata_bytes, plain, physical_caps(plain; metadata=metadata_size - 1)),
            (:logical_bytes, plain, physical_caps(plain; logical=logical_size - 1)),
            (:records, plain, physical_caps(plain; records=1)),
            (:physical_source_bytes, plain, physical_caps(plain; physical=filesize(plain) + 3)),
            (:physical_source_bytes, zstd, physical_caps(zstd; physical=16)),
            (:outer_buffer_capacity_bytes, plain, physical_caps(plain; outer=128 * 1024 - 1)),
            (:transcoder_buffer_capacity_bytes, zstd, physical_caps(zstd; transcoder=32 * 1024 - 1)),
        ]
        for (bound, path, caps) in cases
            caught = try
                DBN.foreach_physical_cmbp1(_ -> DBN.PHYSICAL_CONTINUE,
                    path, caps, physical_binding(path)); nothing
            catch error; error end
            @test caught isa DBN.PhysicalStreamTerminalErrorV1
            @test caught.kind == :resource && caught.bound == bound
            @test caught.observed > caught.limit && caught.source_closed
            @test caught.counters.outer_buffer_unread_bytes <= caught.counters.outer_buffer_capacity_bytes
        end

        plain_bytes = read(plain); record_offset = first_record_offset(plain_bytes)
        for (bound, extra, expected_count) in
            ((:logical_bytes, 16, 16), (:logical_bytes, 23, 23),
             (:physical_source_bytes, 16, 16), (:physical_source_bytes, 23, 23))
            caps = bound == :logical_bytes ?
                physical_caps(plain; logical=metadata_total + extra) :
                physical_caps(plain; physical=4 + metadata_total + extra)
            caught = try
                DBN.foreach_physical_cmbp1(_ -> DBN.PHYSICAL_CONTINUE,
                    plain, caps, physical_binding(plain)); nothing
            catch error; error end
            @test caught.kind == :resource && caught.bound == bound
            @test caught.ordinal == 0 && caught.partial_bytes_count == expected_count
            @test caught.partial_bytes_sha256 == bytes2hex(sha256(
                plain_bytes[record_offset:record_offset + expected_count - 1]))
            @test caught.source_closed
        end
        invalid = copy(read(plain)); invalid[1] = 0x00
        invalid_path = joinpath(directory, "invalid.dbn"); write(invalid_path, invalid)
        caught = try
            DBN.foreach_physical_cmbp1(_ -> DBN.PHYSICAL_CONTINUE, invalid_path,
                physical_caps(invalid_path), physical_binding(plain)); nothing
        catch error; error end
        @test caught isa DBN.PhysicalStreamTerminalErrorV1
        @test caught.kind == :format && caught.reason == :invalid_magic && caught.source_closed
        @test caught.partial_bytes_count == 8 && caught.partial_bytes_sha256 !== nothing

        missing = joinpath(directory, "missing.dbn")
        caught = try
            DBN.foreach_physical_cmbp1(_ -> DBN.PHYSICAL_CONTINUE, missing,
                physical_caps(plain), physical_binding(plain)); nothing
        catch error; error end
        @test caught isa DBN.PhysicalStreamTerminalErrorV1
        @test caught.kind == :source && caught.reason == :source_open_failure
        @test caught.source_closed && caught.ordinal == 0

        corrupt_zstd = joinpath(directory, "corrupt.dbn.zst")
        write(corrupt_zstd, UInt8[0x28, 0xb5, 0x2f, 0xfd, 0xff, 0xff, 0xff, 0xff])
        caught = try
            DBN.foreach_physical_cmbp1(_ -> DBN.PHYSICAL_CONTINUE, corrupt_zstd,
                physical_caps(corrupt_zstd), physical_binding(plain)); nothing
        catch error; error end
        @test caught isa DBN.PhysicalStreamTerminalErrorV1
        @test caught.kind == :format && caught.reason == :decompression_failure
        @test caught.source_closed && caught.counters.physical_source_bytes_pulled > 0

        caught = try
            DBN._foreach_physical_cmbp1(_ -> DBN.PHYSICAL_CONTINUE, plain,
                physical_caps(plain), physical_binding(plain),
                value -> CloseThrowIO(open(value, "r"))); nothing
        catch error; error end
        @test caught isa DBN.PhysicalStreamTerminalErrorV1
        @test caught.kind == :cleanup && caught.reason == :source_close_failure
        @test caught.source_closed && caught.counters.logical_bytes_consumed == logical_size
    end
end
