@enum DBNStreamDecision::UInt8 begin
    DBN_STREAM_CONTINUE = 0
    DBN_STREAM_STOP = 1
end

struct DBNStreamLimits
    max_metadata_bytes::Int64
    max_record_bytes::Int64
    max_logical_bytes::Int64
    max_source_bytes::Int64
    max_records::UInt64

    function DBNStreamLimits(
        max_metadata_bytes::Integer,
        max_record_bytes::Integer,
        max_logical_bytes::Integer,
        max_source_bytes::Integer,
        max_records::Integer,
    )
        max_metadata_bytes > 0 || throw(ArgumentError("max_metadata_bytes must be positive"))
        max_record_bytes >= 16 || throw(ArgumentError("max_record_bytes must be at least 16"))
        max_logical_bytes >= 8 || throw(ArgumentError("max_logical_bytes must be at least 8"))
        max_source_bytes >= 4 || throw(ArgumentError("max_source_bytes must be at least 4"))
        max_records > 0 || throw(ArgumentError("max_records must be positive"))
        return new(
            Int64(max_metadata_bytes),
            Int64(max_record_bytes),
            Int64(max_logical_bytes),
            Int64(max_source_bytes),
            UInt64(max_records),
        )
    end
end

struct DBNRawRecordHeader
    length_units::UInt8
    rtype_raw::UInt8
    publisher_id::UInt16
    instrument_id::UInt32
    ts_event::Int64
    raw_bytes::NTuple{16,UInt8}
end

"""
Position of one completely consumed DBN record.

`source_bytes_pulled` is the underlying source-pull high-watermark observed after
the record was consumed. It may include decompressor or `BufferedReader`
read-ahead and is not a per-record physical byte boundary.
"""
struct DBNRecordPosition
    ordinal::UInt64
    logical_start_offset::Int64
    logical_end_offset::Int64
    source_bytes_pulled::Int64
end

struct DBNSkippedRecord
    header::DBNRawRecordHeader
    reason::Symbol
end

"""
Terminal accounting for a bounded DBN stream attempt.

`source_bytes_pulled` is the final underlying source-pull high-watermark. It may
include decompressor or `BufferedReader` read-ahead and must not be interpreted
as a physical boundary for `last_position` or any individual record.
"""
struct DBNStreamSummary
    metadata::Union{Metadata,Nothing}
    compressed::Bool
    terminal_reason::Symbol
    eof_reached::Bool
    source_closed::Bool
    records_seen::UInt64
    data_records::UInt64
    control_records::UInt64
    skipped_records::UInt64
    unknown_records::UInt64
    logical_bytes_consumed::Int64
    source_bytes_pulled::Int64
    last_position::Union{DBNRecordPosition,Nothing}
end

struct DBNStreamTerminalError <: Exception
    reason::Symbol
    summary::DBNStreamSummary
    cause::Any
    cleanup_error::Any
    bound::Union{Symbol,Nothing}
    limit::Union{Int64,UInt64,Nothing}
    observed::Union{Int64,UInt64,Nothing}
end

function Base.showerror(io::IO, error::DBNStreamTerminalError)
    print(io, "bounded DBN stream terminated: ", error.reason)
    if error.bound !== nothing
        print(io, " (", error.bound, " limit=", error.limit, ", observed=", error.observed, ")")
    end
    error.cause === nothing || print(io, ": ", sprint(showerror, error.cause))
    error.cleanup_error === nothing ||
        print(io, "; cleanup failed: ", sprint(showerror, error.cleanup_error))
end

struct _DBNBoundedFailure <: Exception
    reason::Symbol
    cause::Any
    bound::Union{Symbol,Nothing}
    limit::Union{Int64,UInt64,Nothing}
    observed::Union{Int64,UInt64,Nothing}
end

_DBNBoundedFailure(reason::Symbol, cause=nothing) =
    _DBNBoundedFailure(reason, cause, nothing, nothing, nothing)
_dbn_limit_failure(reason, bound, limit, observed) =
    _DBNBoundedFailure(reason, nothing, bound, limit, observed)

mutable struct _DBNSourceLimitIO{T<:IO} <: IO
    io::T
    limit::Int64
    bytes_read::Int64
    closed::Bool
end

Base.isopen(io::_DBNSourceLimitIO) = !io.closed && isopen(io.io)

function Base.close(io::_DBNSourceLimitIO)
    io.closed && return nothing
    io.closed = true
    return close(io.io)
end

function Base.eof(io::_DBNSourceLimitIO)
    if io.bytes_read == io.limit
        eof(io.io) && return true
        throw(_dbn_limit_failure(:source_bytes_limit, :source_bytes, io.limit, io.bytes_read + 1))
    end
    return eof(io.io)
end

function Base.bytesavailable(io::_DBNSourceLimitIO)
    remaining = max(Int64(0), io.limit - io.bytes_read)
    remaining == 0 && return eof(io) ? 0 : 0
    return min(Int(remaining), bytesavailable(io.io))
end

function Base.read(io::_DBNSourceLimitIO, ::Type{UInt8})
    io.bytes_read == io.limit && eof(io)
    byte = read(io.io, UInt8)
    io.bytes_read += 1
    return byte
end

function Base.readbytes!(io::_DBNSourceLimitIO, buffer::AbstractVector{UInt8}, count::Integer)
    count <= 0 && return 0
    remaining = max(Int64(0), io.limit - io.bytes_read)
    remaining == 0 && return eof(io) ? 0 : 0
    permitted = min(Int64(count), remaining)
    read_count = readbytes!(io.io, buffer, Int(permitted))
    io.bytes_read += read_count
    return read_count
end

mutable struct _DBNPrefixReplayIO{T<:IO} <: IO
    prefix::Vector{UInt8}
    next_index::Int
    io::T
    closed::Bool
end

Base.isopen(io::_DBNPrefixReplayIO) = !io.closed && isopen(io.io)
Base.eof(io::_DBNPrefixReplayIO) = io.next_index > length(io.prefix) && eof(io.io)

function Base.close(io::_DBNPrefixReplayIO)
    io.closed && return nothing
    io.closed = true
    return close(io.io)
end

function Base.bytesavailable(io::_DBNPrefixReplayIO)
    prefix_available = max(0, length(io.prefix) - io.next_index + 1)
    return prefix_available + bytesavailable(io.io)
end

function Base.read(io::_DBNPrefixReplayIO, ::Type{UInt8})
    if io.next_index <= length(io.prefix)
        byte = io.prefix[io.next_index]
        io.next_index += 1
        return byte
    end
    return read(io.io, UInt8)
end

function Base.readbytes!(io::_DBNPrefixReplayIO, buffer::AbstractVector{UInt8}, count::Integer)
    count <= 0 && return 0
    copied = 0
    while copied < count && io.next_index <= length(io.prefix)
        copied += 1
        buffer[copied] = io.prefix[io.next_index]
        io.next_index += 1
    end
    copied == count && return copied
    return copied + readbytes!(io.io, view(buffer, (copied + 1):length(buffer)), count - copied)
end

mutable struct _DBNBoundedState
    metadata::Union{Metadata,Nothing}
    compressed::Bool
    terminal_reason::Symbol
    eof_reached::Bool
    records_seen::UInt64
    data_records::UInt64
    control_records::UInt64
    skipped_records::UInt64
    unknown_records::UInt64
    last_position::Union{DBNRecordPosition,Nothing}
end

_DBNBoundedState() =
    _DBNBoundedState(nothing, false, :not_started, false, 0, 0, 0, 0, 0, nothing)

function _dbn_bounded_summary(state, reader, source, source_closed)
    logical = reader === nothing ? Int64(0) : Int64(reader.total_read)
    source_bytes = source === nothing ? Int64(0) : source.bytes_read
    return DBNStreamSummary(
        state.metadata,
        state.compressed,
        state.terminal_reason,
        state.eof_reached,
        source_closed,
        state.records_seen,
        state.data_records,
        state.control_records,
        state.skipped_records,
        state.unknown_records,
        logical,
        source_bytes,
        state.last_position,
    )
end

function _dbn_checked_add(left::Int64, right::Int64)
    try
        return Base.checked_add(left, right)
    catch error
        throw(_DBNBoundedFailure(:logical_bytes_overflow, error))
    end
end

function _dbn_stream_read(reader, count::Integer, compressed::Bool)
    try
        return read(reader, count)
    catch error
        error isa _DBNBoundedFailure && rethrow()
        throw(_DBNBoundedFailure(compressed ? :decompression_failure : :source_read_failure, error))
    end
end

function _dbn_stream_eof(reader, compressed::Bool)
    try
        return eof(reader)
    catch error
        error isa _DBNBoundedFailure && rethrow()
        throw(_DBNBoundedFailure(compressed ? :decompression_failure : :source_read_failure, error))
    end
end

function _dbn_little_endian(::Type{T}, bytes, first_index) where {T}
    width = sizeof(T)
    return ltoh(read(IOBuffer(bytes[first_index:(first_index + width - 1)]), T))
end

function _dbn_raw_header(bytes::Vector{UInt8})
    return DBNRawRecordHeader(
        bytes[1],
        bytes[2],
        _dbn_little_endian(UInt16, bytes, 3),
        _dbn_little_endian(UInt32, bytes, 5),
        _dbn_little_endian(Int64, bytes, 9),
        Tuple(bytes),
    )
end

function _dbn_known_rtype(raw::UInt8)
    try
        return RType.T(raw)
    catch error
        error isa ArgumentError || rethrow()
        return nothing
    end
end

function _dbn_record_header(raw::DBNRawRecordHeader, rtype::RType.T)
    return RecordHeader(
        raw.length_units,
        rtype,
        raw.publisher_id,
        raw.instrument_id,
        raw.ts_event,
    )
end

function _dbn_matches_type(rtype::RType.T, ::Type{T}) where {T<:DBNRecord}
    expected = _type_to_rtype_stream(T)
    if T === OHLCVMsg
        return rtype in (
            RType.OHLCV_1S_MSG,
            RType.OHLCV_1M_MSG,
            RType.OHLCV_1H_MSG,
            RType.OHLCV_1D_MSG,
        )
    end
    return rtype == expected
end

_dbn_is_control(rtype::RType.T) = rtype in (
    RType.ERROR_MSG,
    RType.SYSTEM_MSG,
    RType.SYMBOL_MAPPING_MSG,
)

function _dbn_callback_decision(callback, argument, position)
    decision = try
        callback(argument, position)
    catch error
        throw(_DBNBoundedFailure(:callback_failure, error))
    end
    decision isa DBNStreamDecision || throw(
        _DBNBoundedFailure(
            :invalid_callback_decision,
            ArgumentError("bounded stream callback must return DBNStreamDecision"),
        ),
    )
    return decision
end

function _dbn_metadata_callback_decision(callback, metadata)
    decision = try
        callback(metadata)
    catch error
        throw(_DBNBoundedFailure(:metadata_callback_failure, error))
    end
    decision isa DBNStreamDecision || throw(
        _DBNBoundedFailure(
            :invalid_metadata_callback_decision,
            ArgumentError("bounded metadata callback must return DBNStreamDecision"),
        ),
    )
    return decision
end

function _dbn_read_metadata!(reader, state, limits)
    prefix = _dbn_stream_read(reader, 8, state.compressed)
    length(prefix) == 8 || throw(_DBNBoundedFailure(:truncated_metadata_prefix))
    prefix[1:3] == UInt8[0x44, 0x42, 0x4e] ||
        throw(_DBNBoundedFailure(:invalid_magic))
    metadata_length = Int64(_dbn_little_endian(UInt32, prefix, 5))
    metadata_length <= limits.max_metadata_bytes || throw(
        _dbn_limit_failure(
            :metadata_bytes_limit,
            :metadata_bytes,
            limits.max_metadata_bytes,
            metadata_length,
        ),
    )
    metadata_end = _dbn_checked_add(Int64(8), metadata_length)
    metadata_end <= limits.max_logical_bytes || throw(
        _dbn_limit_failure(
            :logical_bytes_limit,
            :logical_bytes,
            limits.max_logical_bytes,
            metadata_end,
        ),
    )
    metadata_bytes = _dbn_stream_read(reader, metadata_length, state.compressed)
    length(metadata_bytes) == metadata_length ||
        throw(_DBNBoundedFailure(:truncated_metadata))

    temporary = DBNDecoder(IOBuffer(vcat(prefix, metadata_bytes)))
    try
        read_header!(temporary)
    catch error
        throw(_DBNBoundedFailure(:metadata_decode_failure, error))
    end
    state.metadata = temporary.metadata
    return temporary
end

function _dbn_read_raw_header(reader, state, limits)
    logical_start = Int64(reader.total_read)
    first_end = _dbn_checked_add(logical_start, Int64(1))
    first_end <= limits.max_logical_bytes || throw(
        _dbn_limit_failure(
            :logical_bytes_limit,
            :logical_bytes,
            limits.max_logical_bytes,
            first_end,
        ),
    )
    first_byte = _dbn_stream_read(reader, 1, state.compressed)
    isempty(first_byte) && return nothing
    record_bytes = Int64(first_byte[1]) * 4
    record_bytes >= 16 || throw(_DBNBoundedFailure(:invalid_record_length))
    record_bytes <= limits.max_record_bytes || throw(
        _dbn_limit_failure(
            :record_bytes_limit,
            :record_bytes,
            limits.max_record_bytes,
            record_bytes,
        ),
    )
    logical_end = _dbn_checked_add(logical_start, record_bytes)
    logical_end <= limits.max_logical_bytes || throw(
        _dbn_limit_failure(
            :logical_bytes_limit,
            :logical_bytes,
            limits.max_logical_bytes,
            logical_end,
        ),
    )
    remaining = _dbn_stream_read(reader, 15, state.compressed)
    length(remaining) == 15 || throw(_DBNBoundedFailure(:truncated_record_header))
    bytes = vcat(first_byte, remaining)
    raw = _dbn_raw_header(bytes)
    return raw, logical_start, logical_end, record_bytes
end

function _dbn_drain_body(reader, body_bytes, state, reason)
    body = _dbn_stream_read(reader, body_bytes, state.compressed)
    length(body) == body_bytes || throw(_DBNBoundedFailure(reason))
    return nothing
end

function _dbn_decode_record(decoder, ::Type{T}, header, logical_end, state) where {T<:DBNRecord}
    record = try
        _read_typed_record_stream(decoder, T, header)
    catch error
        error isa _DBNBoundedFailure && rethrow()
        error isa EOFError && throw(_DBNBoundedFailure(:truncated_record_body, error))
        throw(_DBNBoundedFailure(:record_decode_failure, error))
    end
    record === nothing && throw(_DBNBoundedFailure(:record_decode_failure))
    decoder.io.total_read == logical_end || throw(
        _DBNBoundedFailure(
            :record_length_mismatch,
            ArgumentError("record decoder did not consume the declared record length"),
        ),
    )
    return record
end

function _dbn_decode_control(decoder, header, rtype, logical_end, state)
    record = try
        read_record_dispatch(decoder, header, rtype)
    catch error
        error isa _DBNBoundedFailure && rethrow()
        error isa EOFError && throw(_DBNBoundedFailure(:truncated_record_body, error))
        throw(_DBNBoundedFailure(:record_decode_failure, error))
    end
    record === nothing && throw(_DBNBoundedFailure(:record_decode_failure))
    decoder.io.total_read == logical_end || throw(
        _DBNBoundedFailure(
            :record_length_mismatch,
            ArgumentError("control decoder did not consume the declared record length"),
        ),
    )
    return record
end

function _dbn_bounded_record_loop!(
    decoder,
    source,
    state,
    limits,
    ::Type{T},
    data_callback,
    control_callback,
    skipped_callback,
) where {T<:DBNRecord}
    reader = decoder.io
    while true
        if state.records_seen == limits.max_records
            if _dbn_stream_eof(reader, state.compressed)
                state.eof_reached = true
                state.terminal_reason = :eof
                return
            end
            throw(
                _dbn_limit_failure(
                    :record_count_limit,
                    :records,
                    limits.max_records,
                    state.records_seen + 1,
                ),
            )
        end
        if _dbn_stream_eof(reader, state.compressed)
            state.eof_reached = true
            state.terminal_reason = :eof
            return
        end

        raw, logical_start, logical_end, record_bytes =
            _dbn_read_raw_header(reader, state, limits)
        body_bytes = record_bytes - 16
        rtype = _dbn_known_rtype(raw.rtype_raw)
        category = :skipped
        result = nothing

        if rtype === nothing
            _dbn_drain_body(reader, body_bytes, state, :truncated_record_body)
            result = DBNSkippedRecord(raw, :unknown_rtype)
            state.skipped_records += 1
            state.unknown_records += 1
        else
            header = _dbn_record_header(raw, rtype)
            if _dbn_is_control(rtype)
                category = :control
                result = _dbn_decode_control(decoder, header, rtype, logical_end, state)
                state.control_records += 1
            elseif _dbn_matches_type(rtype, T)
                record_bytes == sizeof(T) || throw(
                    _DBNBoundedFailure(
                        :record_length_mismatch,
                        ArgumentError("declared record length differs from requested record type"),
                    ),
                )
                category = :data
                result = _dbn_decode_record(decoder, T, header, logical_end, state)
                state.data_records += 1
            else
                _dbn_drain_body(reader, body_bytes, state, :truncated_record_body)
                result = DBNSkippedRecord(raw, :unmatched_rtype)
                state.skipped_records += 1
            end
        end

        position = DBNRecordPosition(
            state.records_seen,
            logical_start,
            logical_end,
            source.bytes_read,
        )
        state.records_seen += 1
        state.last_position = position
        callback = category === :data ? data_callback :
                   category === :control ? control_callback : skipped_callback
        if _dbn_callback_decision(callback, result, position) == DBN_STREAM_STOP
            state.terminal_reason = :callback_stop
            return
        end
    end
end

function _dbn_merge_cleanup_error(first_error, next_error)
    first_error === nothing && return next_error
    return CompositeException(Any[first_error, next_error])
end

function _dbn_close_chain(reader, base)
    first_error = nothing
    if reader !== nothing
        try
            close(reader)
        catch error
            first_error = error
        end
    end
    should_close_base = base !== nothing
    if base !== nothing
        try
            should_close_base = isopen(base)
        catch error
            first_error = _dbn_merge_cleanup_error(first_error, error)
            should_close_base = true
        end
    end
    if should_close_base
        try
            close(base)
        catch error
            first_error = _dbn_merge_cleanup_error(first_error, error)
        end
    end
    return first_error
end

function _dbn_source_closed(base)
    base === nothing && return true, nothing
    try
        return !isopen(base), nothing
    catch error
        return false, error
    end
end

function _foreach_record_with_control_bounded(
    metadata_callback,
    data_callback,
    control_callback,
    skipped_callback,
    filename::AbstractString,
    ::Type{T},
    limits::DBNStreamLimits;
    source_opener=open,
) where {T<:DBNRecord}
    state = _DBNBoundedState()
    base = nothing
    source = nothing
    reader = nothing
    primary_error = nothing
    cleanup_error = nothing
    bound = nothing
    limit = nothing
    observed = nothing

    try
        base = source_opener(filename, "r")
        source = _DBNSourceLimitIO(base, limits.max_source_bytes, 0, false)
        probe = read(source, 4)
        length(probe) == 4 || throw(_DBNBoundedFailure(:truncated_source_prefix))
        replay = _DBNPrefixReplayIO(probe, 1, source, false)
        state.compressed = probe == UInt8[0x28, 0xb5, 0x2f, 0xfd] ||
                           endswith(lowercase(String(filename)), ".zst")
        decoded_io = state.compressed ?
                     TranscodingStream(ZstdDecompressor(), replay) : replay
        reader = BufferedReader(decoded_io)
        template = _dbn_read_metadata!(reader, state, limits)
        decoder = DBNDecoder(reader)
        decoder.header = template.header
        decoder.metadata = template.metadata
        decoder.upgrade_policy = template.upgrade_policy
        decoder.string_cache = template.string_cache
        if _dbn_metadata_callback_decision(metadata_callback, state.metadata) ==
           DBN_STREAM_STOP
            state.terminal_reason = :metadata_callback_stop
        else
        _dbn_bounded_record_loop!(
            decoder,
            source,
            state,
            limits,
            T,
            data_callback,
            control_callback,
            skipped_callback,
        )
        end
    catch error
        primary_error = error
        if error isa _DBNBoundedFailure
            state.terminal_reason = error.reason
            bound = error.bound
            limit = error.limit
            observed = error.observed
        elseif base === nothing
            state.terminal_reason = :source_open_failure
        else
            state.terminal_reason = :unexpected_stream_failure
        end
    finally
        cleanup_error = _dbn_close_chain(reader, base)
    end

    primary_error === nothing && cleanup_error !== nothing &&
        (state.terminal_reason = :source_close_failure)
    source_closed, probe_error = _dbn_source_closed(base)
    if probe_error !== nothing
        cleanup_error = _dbn_merge_cleanup_error(cleanup_error, probe_error)
        primary_error === nothing && (state.terminal_reason = :source_close_failure)
    end
    summary = _dbn_bounded_summary(state, reader, source, source_closed)
    if primary_error !== nothing
        cause = primary_error isa _DBNBoundedFailure ? primary_error.cause : primary_error
        throw(
            DBNStreamTerminalError(
                state.terminal_reason,
                summary,
                cause,
                cleanup_error,
                bound,
                limit,
                observed,
            ),
        )
    elseif cleanup_error !== nothing
        throw(
            DBNStreamTerminalError(
                :source_close_failure,
                summary,
                nothing,
                cleanup_error,
                nothing,
                nothing,
                nothing,
            ),
        )
    end
    return summary
end

function _foreach_record_with_control_bounded(
    data_callback,
    control_callback,
    skipped_callback,
    filename::AbstractString,
    ::Type{T},
    limits::DBNStreamLimits;
    source_opener=open,
) where {T<:DBNRecord}
    return _foreach_record_with_control_bounded(
        _ -> DBN_STREAM_CONTINUE,
        data_callback,
        control_callback,
        skipped_callback,
        filename,
        T,
        limits;
        source_opener=source_opener,
    )
end

function foreach_record_with_control(
    metadata_callback,
    data_callback,
    control_callback,
    skipped_callback,
    filename::AbstractString,
    ::Type{T},
    limits::DBNStreamLimits,
) where {T<:DBNRecord}
    return _foreach_record_with_control_bounded(
        metadata_callback,
        data_callback,
        control_callback,
        skipped_callback,
        filename,
        T,
        limits,
    )
end

function foreach_record_with_control(
    data_callback,
    control_callback,
    skipped_callback,
    filename::AbstractString,
    ::Type{T},
    limits::DBNStreamLimits,
) where {T<:DBNRecord}
    return _foreach_record_with_control_bounded(
        data_callback,
        control_callback,
        skipped_callback,
        filename,
        T,
        limits,
    )
end

function foreach_record(
    data_callback,
    skipped_callback,
    filename::AbstractString,
    ::Type{T},
    limits::DBNStreamLimits,
) where {T<:DBNRecord}
    return foreach_record_with_control(
        data_callback,
        (_, _) -> DBN_STREAM_CONTINUE,
        skipped_callback,
        filename,
        T,
        limits,
    )
end

function foreach_cmbp1(
    metadata_callback,
    data_callback,
    control_callback,
    skipped_callback,
    filename::AbstractString,
    limits::DBNStreamLimits,
)
    return foreach_record_with_control(
        metadata_callback,
        data_callback,
        control_callback,
        skipped_callback,
        filename,
        CMBP1Msg,
        limits,
    )
end

function foreach_cmbp1(
    data_callback,
    control_callback,
    skipped_callback,
    filename::AbstractString,
    limits::DBNStreamLimits,
)
    return foreach_record_with_control(
        data_callback,
        control_callback,
        skipped_callback,
        filename,
        CMBP1Msg,
        limits,
    )
end
