using CodecZstd

const BOUNDED_DBE = DatabentoBinaryEncoding

function bounded_trade(instrument_id, ts_event)
    return TradeMsg(
        RecordHeader(
            UInt8(sizeof(TradeMsg) / 4),
            RType.MBP_0_MSG,
            UInt16(1),
            UInt32(instrument_id),
            Int64(ts_event),
        ),
        Int64(100_000_000_000),
        UInt32(3),
        Action.TRADE,
        Side.BID,
        UInt8(0),
        UInt8(0),
        Int64(ts_event),
        Int32(0),
        UInt32(instrument_id),
    )
end

function bounded_system(ts_event)
    return SystemMsg(
        RecordHeader(
            UInt8(20),
            RType.SYSTEM_MSG,
            UInt16(0),
            UInt32(0),
            Int64(ts_event),
        ),
        "heartbeat",
        "",
    )
end

bounded_metadata_length(bytes) = Int(ltoh(read(IOBuffer(bytes[5:8]), UInt32)))

function bounded_unknown_record()
    io = IOBuffer()
    write(io, UInt8(8))
    write(io, UInt8(0xfe))
    write(io, htol(UInt16(0x1234)))
    write(io, htol(UInt32(0x12345678)))
    write(io, htol(Int64(123456789)))
    write(io, fill(UInt8(0xa5), 16))
    return take!(io)
end

function bounded_fixture(directory)
    source = joinpath(directory, "source.dbn")
    metadata = Metadata(
        UInt8(3),
        "XNAS.ITCH",
        Schema.TRADES,
        Int64(0),
        nothing,
        nothing,
        SType.RAW_SYMBOL,
        SType.INSTRUMENT_ID,
        false,
        String[],
        String[],
        String[],
        Tuple{String,String,Int64,Int64}[],
    )
    write_dbn(
        source,
        metadata,
        [bounded_trade(1, 100), bounded_system(101), bounded_trade(2, 102)],
    )
    original = read(source)
    metadata_end = 8 + bounded_metadata_length(original)
    first_end = metadata_end + sizeof(TradeMsg)
    logical = vcat(
        original[1:first_end],
        bounded_unknown_record(),
        original[(first_end + 1):end],
    )
    plain = joinpath(directory, "mixed.dbn")
    compressed = joinpath(directory, "mixed.dbn.zst")
    write(plain, logical)
    write(compressed, transcode(ZstdCompressor, logical))
    return logical, (plain, compressed), metadata_end
end

function bounded_limits(
    path,
    logical;
    metadata=1_000_000,
    record=1_024,
    logical_limit=length(logical),
    source=filesize(path),
    records=100,
)
    return DBNStreamLimits(metadata, record, logical_limit, source, records)
end

function bounded_capture(path, logical; limits=bounded_limits(path, logical))
    data = Any[]
    controls = Any[]
    skipped = Any[]
    summary = foreach_record_with_control(
        (record, position) -> (push!(data, (record, position)); DBN_STREAM_CONTINUE),
        (record, position) -> (push!(controls, (record, position)); DBN_STREAM_CONTINUE),
        (record, position) -> (push!(skipped, (record, position)); DBN_STREAM_CONTINUE),
        path,
        TradeMsg,
        limits,
    )
    return summary, data, controls, skipped
end

function bounded_terminal_error(f)
    try
        f()
    catch error
        @test error isa DBNStreamTerminalError
        return error
    end
    error("expected DBNStreamTerminalError")
end

mutable struct BoundedCloseThrowIO{T<:IO} <: IO
    io::T
end

mutable struct BoundedSpyIO{T<:IO} <: IO
    io::T
    reads::Int
    eof_calls::Int
end

Base.isopen(io::BoundedSpyIO) = isopen(io.io)
function Base.eof(io::BoundedSpyIO)
    io.eof_calls += 1
    return eof(io.io)
end
Base.bytesavailable(io::BoundedSpyIO) = bytesavailable(io.io)
function Base.read(io::BoundedSpyIO, ::Type{UInt8})
    io.reads += 1
    return read(io.io, UInt8)
end
function Base.readbytes!(io::BoundedSpyIO, buffer::AbstractVector{UInt8}, count::Integer)
    io.reads += 1
    return readbytes!(io.io, buffer, count)
end
Base.close(io::BoundedSpyIO) = close(io.io)

mutable struct BoundedIsopenThrowIO{T<:IO} <: IO
    io::T
end

Base.isopen(::BoundedIsopenThrowIO) = error("injected isopen failure")
Base.eof(io::BoundedIsopenThrowIO) = eof(io.io)
Base.bytesavailable(io::BoundedIsopenThrowIO) = bytesavailable(io.io)
Base.read(io::BoundedIsopenThrowIO, ::Type{UInt8}) = read(io.io, UInt8)
Base.readbytes!(io::BoundedIsopenThrowIO, buffer::AbstractVector{UInt8}, count::Integer) =
    readbytes!(io.io, buffer, count)
Base.close(io::BoundedIsopenThrowIO) = close(io.io)

Base.isopen(io::BoundedCloseThrowIO) = isopen(io.io)
Base.eof(io::BoundedCloseThrowIO) = eof(io.io)
Base.bytesavailable(io::BoundedCloseThrowIO) = bytesavailable(io.io)
Base.read(io::BoundedCloseThrowIO, ::Type{UInt8}) = read(io.io, UInt8)
Base.readbytes!(io::BoundedCloseThrowIO, buffer::AbstractVector{UInt8}, count::Integer) =
    readbytes!(io.io, buffer, count)
function Base.close(io::BoundedCloseThrowIO)
    close(io.io)
    error("injected close failure")
end

@testset "Bounded record streaming" begin
    @test_throws ArgumentError DBNStreamLimits(0, 16, 8, 4, 1)
    @test_throws ArgumentError DBNStreamLimits(1, 15, 8, 4, 1)
    @test_throws ArgumentError DBNStreamLimits(1, 16, 7, 4, 1)
    @test_throws ArgumentError DBNStreamLimits(1, 16, 8, 3, 1)
    @test_throws ArgumentError DBNStreamLimits(1, 16, 8, 4, 0)

    mktempdir() do directory
        logical, paths, metadata_end = bounded_fixture(directory)
        for path in paths
            @testset "plain/Zstd complete and callback stop: $(basename(path))" begin
                summary, data, controls, skipped = bounded_capture(path, logical)
                @test summary.terminal_reason == :eof
                @test summary.eof_reached
                @test summary.source_closed
                @test summary.records_seen == 4
                @test summary.data_records == 2
                @test summary.control_records == 1
                @test summary.skipped_records == 1
                @test summary.unknown_records == 1
                @test summary.logical_bytes_consumed == length(logical)
                @test summary.source_bytes_pulled == filesize(path)
                @test length(data) == 2
                @test length(controls) == 1
                @test length(skipped) == 1
                @test skipped[1][1].reason == :unknown_rtype
                @test skipped[1][1].header.rtype_raw == 0xfe
                @test skipped[1][1].header.publisher_id == 0x1234
                @test skipped[1][1].header.instrument_id == 0x12345678
                @test skipped[1][1].header.raw_bytes == Tuple(bounded_unknown_record()[1:16])
                @test data[1][2].ordinal == 0
                @test skipped[1][2].ordinal == 1
                @test controls[1][2].ordinal == 2
                @test data[2][2].ordinal == 3
                @test summary.last_position.ordinal == 3

                stopped = foreach_record_with_control(
                    (_, _) -> DBN_STREAM_STOP,
                    (_, _) -> error("control must not run"),
                    (_, _) -> error("skip must not run"),
                    path,
                    TradeMsg,
                    bounded_limits(path, logical),
                )
                @test stopped.terminal_reason == :callback_stop
                @test !stopped.eof_reached
                @test stopped.source_closed
                @test stopped.records_seen == 1
                @test stopped.data_records == 1
                @test stopped.logical_bytes_consumed == metadata_end + sizeof(TradeMsg)

                data_only = Ref(0)
                data_only_summary = foreach_record(
                    (_, _) -> (data_only[] += 1; DBN_STREAM_CONTINUE),
                    (_, _) -> DBN_STREAM_CONTINUE,
                    path,
                    TradeMsg,
                    bounded_limits(path, logical),
                )
                @test data_only[] == 2
                @test data_only_summary.control_records == 1
            end

            @testset "plain/Zstd hard limits: $(basename(path))" begin
                metadata_length = bounded_metadata_length(logical)
                terminal = bounded_terminal_error() do
                    bounded_capture(
                        path,
                        logical;
                        limits=bounded_limits(path, logical; metadata=metadata_length - 1),
                    )
                end
                @test terminal.reason == :metadata_bytes_limit
                @test terminal.bound == :metadata_bytes
                @test terminal.limit == metadata_length - 1
                @test terminal.observed == metadata_length
                @test terminal.summary.source_closed

                terminal = bounded_terminal_error() do
                    bounded_capture(
                        path,
                        logical;
                        limits=bounded_limits(path, logical; record=sizeof(TradeMsg) - 1),
                    )
                end
                @test terminal.reason == :record_bytes_limit
                @test terminal.bound == :record_bytes
                @test terminal.summary.records_seen == 0
                @test terminal.summary.logical_bytes_consumed == metadata_end + 1

                terminal = bounded_terminal_error() do
                    bounded_capture(
                        path,
                        logical;
                        limits=bounded_limits(
                            path,
                            logical;
                            logical_limit=metadata_end + sizeof(TradeMsg) - 1,
                        ),
                    )
                end
                @test terminal.reason == :logical_bytes_limit
                @test terminal.observed == metadata_end + sizeof(TradeMsg)
                @test terminal.summary.logical_bytes_consumed == metadata_end + 1

                terminal = bounded_terminal_error() do
                    bounded_capture(
                        path,
                        logical;
                        limits=bounded_limits(path, logical; records=1),
                    )
                end
                @test terminal.reason == :record_count_limit
                @test terminal.summary.records_seen == 1
                @test terminal.observed == 2

                terminal = bounded_terminal_error() do
                    bounded_capture(
                        path,
                        logical;
                        limits=bounded_limits(path, logical; source=filesize(path) - 1),
                    )
                end
                @test terminal.reason == :source_bytes_limit
                @test terminal.bound == :source_bytes
                @test terminal.summary.source_bytes_pulled == filesize(path) - 1
                @test terminal.summary.source_closed
            end
        end


        @testset "callback stop does not prime or probe another record" begin
            for path in paths
                spy = Ref{Any}(nothing)
                callback_counts = Ref{Tuple{Int,Int}}()
                source_cap = path == paths[1] ?
                             metadata_end + sizeof(TradeMsg) : filesize(path)
                summary = BOUNDED_DBE._foreach_record_with_control_bounded(
                    (_, _) -> begin
                        callback_counts[] = (spy[].reads, spy[].eof_calls)
                        DBN_STREAM_STOP
                    end,
                    (_, _) -> error("control must not run"),
                    (_, _) -> error("skip must not run"),
                    path,
                    TradeMsg,
                    bounded_limits(path, logical; source=source_cap);
                    source_opener=(name, mode) -> begin
                        spy[] = BoundedSpyIO(open(name, mode), 0, 0)
                        spy[]
                    end,
                )
                @test summary.terminal_reason == :callback_stop
                @test summary.records_seen == 1
                @test (spy[].reads, spy[].eof_calls) == callback_counts[]
                path == paths[1] && @test summary.source_bytes_pulled == source_cap
            end
        end

        @testset "plain/Zstd truncation and decompression" begin
            for compressed in (false, true)
                for (label, cutoff, expected) in (
                    ("header", metadata_end + 10, :truncated_record_header),
                    ("body", metadata_end + 20, :truncated_record_body),
                )
                    truncated_logical = logical[1:cutoff]
                    bytes = compressed ?
                            transcode(ZstdCompressor, truncated_logical) : truncated_logical
                    suffix = compressed ? "dbn.zst" : "dbn"
                    path = joinpath(directory, "truncated-$(label).$(suffix)")
                    write(path, bytes)
                    terminal = bounded_terminal_error() do
                        bounded_capture(
                            path,
                            logical;
                            limits=bounded_limits(path, logical),
                        )
                    end
                    @test terminal.reason == expected
                    @test terminal.summary.records_seen == 0
                    @test terminal.summary.source_closed
                end
            end

            corrupt = joinpath(directory, "corrupt.dbn.zst")
            compressed = read(paths[2])
            compressed[end - 3] = xor(compressed[end - 3], 0xff)
            write(corrupt, compressed)
            terminal = bounded_terminal_error() do
                bounded_capture(corrupt, logical; limits=bounded_limits(corrupt, logical))
            end
            @test terminal.reason == :decompression_failure
            @test terminal.summary.source_closed
        end

        @testset "callback and cleanup terminal summaries" begin
            for path in paths
                terminal = bounded_terminal_error() do
                    foreach_record_with_control(
                        (_, _) -> error("injected callback failure"),
                        (_, _) -> DBN_STREAM_CONTINUE,
                        (_, _) -> DBN_STREAM_CONTINUE,
                        path,
                        TradeMsg,
                        bounded_limits(path, logical),
                    )
                end
                @test terminal.reason == :callback_failure
                @test terminal.summary.records_seen == 1
                @test terminal.summary.data_records == 1
                @test terminal.summary.source_closed

                terminal = bounded_terminal_error() do
                    foreach_record_with_control(
                        (_, _) -> nothing,
                        (_, _) -> DBN_STREAM_CONTINUE,
                        (_, _) -> DBN_STREAM_CONTINUE,
                        path,
                        TradeMsg,
                        bounded_limits(path, logical),
                    )
                end
                @test terminal.reason == :invalid_callback_decision
                @test terminal.summary.records_seen == 1
            end

            path = paths[1]
            terminal = bounded_terminal_error() do
                BOUNDED_DBE._foreach_record_with_control_bounded(
                    (_, _) -> DBN_STREAM_CONTINUE,
                    (_, _) -> DBN_STREAM_CONTINUE,
                    (_, _) -> DBN_STREAM_CONTINUE,
                    path,
                    TradeMsg,
                    bounded_limits(path, logical);
                    source_opener=(name, mode) -> BoundedCloseThrowIO(open(name, mode)),
                )
            end
            @test terminal.reason == :source_close_failure
            @test terminal.cleanup_error !== nothing
            @test terminal.summary.eof_reached
            @test terminal.summary.source_closed

            terminal = bounded_terminal_error() do
                BOUNDED_DBE._foreach_record_with_control_bounded(
                    (_, _) -> error("primary callback failure"),
                    (_, _) -> DBN_STREAM_CONTINUE,
                    (_, _) -> DBN_STREAM_CONTINUE,
                    path,
                    TradeMsg,
                    bounded_limits(path, logical);
                    source_opener=(name, mode) -> BoundedCloseThrowIO(open(name, mode)),
                )
            end
            @test terminal.reason == :callback_failure
            @test terminal.cleanup_error !== nothing
            @test terminal.summary.source_closed

            terminal = bounded_terminal_error() do
                BOUNDED_DBE._foreach_record_with_control_bounded(
                    (_, _) -> DBN_STREAM_CONTINUE,
                    (_, _) -> DBN_STREAM_CONTINUE,
                    (_, _) -> DBN_STREAM_CONTINUE,
                    path,
                    TradeMsg,
                    bounded_limits(path, logical);
                    source_opener=(name, mode) -> BoundedIsopenThrowIO(open(name, mode)),
                )
            end
            @test terminal.reason == :source_close_failure
            @test terminal.cleanup_error !== nothing
            @test !terminal.summary.source_closed

            terminal = bounded_terminal_error() do
                BOUNDED_DBE._foreach_record_with_control_bounded(
                    (_, _) -> DBN_STREAM_CONTINUE,
                    (_, _) -> DBN_STREAM_CONTINUE,
                    (_, _) -> DBN_STREAM_CONTINUE,
                    path,
                    TradeMsg,
                    bounded_limits(path, logical);
                    source_opener=(_, _) -> 1,
                )
            end
            @test terminal.reason == :unexpected_stream_failure
            @test terminal.cleanup_error !== nothing
        end

        @testset "metadata gate precedes record callbacks" begin
            for path in paths
                metadata_calls = Ref(00)
                record_calls = Ref(0)
                summary = foreach_record_with_control(
                    metadata -> begin
                        metadata_calls[] += 1
                        @test metadata.dataset == "XNAS.ITCH"
                        DBN_STREAM_CONTINUE
                    end,
                    (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                    (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                    (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                    path,
                    TradeMsg,
                    bounded_limits(path, logical),
                )
                @test metadata_calls[] == 1
                @test record_calls[] == 4
                @test summary.terminal_reason == :eof

                metadata_calls[] = 0
                record_calls[] = 0
                stopped = foreach_record_with_control(
                    _ -> (metadata_calls[] += 1; DBN_STREAM_STOP),
                    (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                    (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                    (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                    path,
                    TradeMsg,
                    bounded_limits(path, logical),
                )
                @test metadata_calls[] == 1
                @test record_calls[] == 0
                @test stopped.terminal_reason == :metadata_callback_stop
                @test !stopped.eof_reached
                @test stopped.source_closed
                @test stopped.records_seen == 0
                @test stopped.logical_bytes_consumed == metadata_end

                record_calls[] = 0
                terminal = bounded_terminal_error() do
                    foreach_record_with_control(
                        _ -> error("metadata rejected"),
                        (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                        (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                        (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                        path,
                        TradeMsg,
                        bounded_limits(path, logical),
                    )
                end
                @test terminal.reason == :metadata_callback_failure
                @test terminal.summary.records_seen == 0
                @test record_calls[] == 0
                @test terminal.summary.source_closed

                terminal = bounded_terminal_error() do
                    foreach_record_with_control(
                        _ -> nothing,
                        (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                        (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                        (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                        path,
                        TradeMsg,
                        bounded_limits(path, logical),
                    )
                end
                @test terminal.reason == :invalid_metadata_callback_decision
                @test terminal.summary.records_seen == 0
                @test record_calls[] == 0
                @test terminal.summary.source_closed
                @test !terminal.summary.eof_reached
                @test terminal.summary.logical_bytes_consumed == metadata_end
            end

            record_calls = Ref(0)
            terminal = bounded_terminal_error() do
                BOUNDED_DBE._foreach_record_with_control_bounded(
                    _ -> error("metadata rejected with cleanup failure"),
                    (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                    (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                    (_, _) -> (record_calls[] += 1; DBN_STREAM_CONTINUE),
                    paths[1],
                    TradeMsg,
                    bounded_limits(paths[1], logical);
                    source_opener=(name, mode) -> BoundedCloseThrowIO(open(name, mode)),
                )
            end
            @test terminal.reason == :metadata_callback_failure
            @test terminal.summary.records_seen == 0
            @test record_calls[] == 0
            @test terminal.cleanup_error !== nothing
            @test terminal.summary.source_closed
        end
    end
end
