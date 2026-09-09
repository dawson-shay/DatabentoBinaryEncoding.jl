"""Callback decision for `foreach_physical_cmbp1`."""
@enum PhysicalStreamDecisionV1::UInt8 begin
    PHYSICAL_CONTINUE = 0
    PHYSICAL_RETAIN_LOOKAHEAD = 1
    PHYSICAL_STOP = 2
end

struct PhysicalStreamCapsV1
    max_metadata_bytes::Int64
    max_logical_bytes::Int64
    max_physical_source_bytes::Int64
    max_records::UInt64

    function PhysicalStreamCapsV1(metadata::Integer, logical::Integer, physical::Integer, records::Integer)
        metadata > 0 || throw(ArgumentError("max_metadata_bytes must be positive"))
        logical > 0 || throw(ArgumentError("max_logical_bytes must be positive"))
        physical >= 4 || throw(ArgumentError("max_physical_source_bytes must be at least four"))
        records > 0 || throw(ArgumentError("max_records must be positive"))
        new(Int64(metadata), Int64(logical), Int64(physical), UInt64(records))
    end
end

PhysicalStreamCapsV1(; max_metadata_bytes, max_logical_bytes, max_physical_source_bytes, max_records) =
    PhysicalStreamCapsV1(max_metadata_bytes, max_logical_bytes, max_physical_source_bytes, max_records)

struct PhysicalStreamCountersV1
    logical_bytes_consumed::Int64
    physical_source_bytes_pulled::Int64
    decompressed_read_ahead_bytes::Int64
end

struct PhysicalRecordHeaderV1
    length_units::Union{UInt8,Nothing}
    rtype_byte::Union{UInt8,Nothing}
    publisher_id::Union{UInt16,Nothing}
    instrument_id::Union{UInt32,Nothing}
    ts_event::Union{Int64,Nothing}
end

struct PhysicalRecordEventV1
    ordinal::UInt64
    disposition::Symbol
    reason::Symbol
    header::PhysicalRecordHeaderV1
    raw_action_byte::Union{UInt8,Nothing}
    raw_side_byte::Union{UInt8,Nothing}
    record::Union{DBNRecord,Nothing}
    logical_start_offset::Int64
    logical_end_offset::Int64
    counters::PhysicalStreamCountersV1
end

struct PhysicalStreamSummaryV1
    dbn_version::UInt8
    metadata_length::UInt32
    schema_raw::UInt16
    compressed::Bool
    records_read::UInt64
    disposition_counts::Dict{Symbol,UInt64}
    retained_lookahead::Union{PhysicalRecordEventV1,Nothing}
    terminal_reason::Symbol
    eof_reached::Bool
    source_closed::Bool
    counters::PhysicalStreamCountersV1
end

mutable struct PhysicalStreamLimitError <: Exception
    bound::Symbol
    limit::Int64
    observed::Int64
    ordinal::UInt64
    counters::PhysicalStreamCountersV1
    source_closed::Bool
end

function Base.showerror(io::IO, error::PhysicalStreamLimitError)
    print(io, "physical stream ", error.bound, " cap exceeded: observed ", error.observed,
          ", limit ", error.limit, ", ordinal ", error.ordinal)
end

struct PhysicalStreamFormatError <: Exception
    reason::Symbol
    detail::String
end
Base.showerror(io::IO, error::PhysicalStreamFormatError) =
    print(io, "physical stream format error (", error.reason, "): ", error.detail)

mutable struct _PhysicalCappedIO{T<:IO} <: IO
    io::T
    limit::Int64
    bytes_read::Int64
    closed::Bool
end
Base.isopen(io::_PhysicalCappedIO) = !io.closed && isopen(io.io)
Base.eof(io::_PhysicalCappedIO) = eof(io.io)
Base.position(io::_PhysicalCappedIO) = io.bytes_read
function Base.bytesavailable(io::_PhysicalCappedIO)
    remaining = io.limit - io.bytes_read
    if remaining <= 0
        eof(io.io) && return 0
        throw(PhysicalStreamLimitError(:physical_source_bytes, io.limit, io.bytes_read + 1,
            0, PhysicalStreamCountersV1(0, io.bytes_read, 0), false))
    end
    min(bytesavailable(io.io), remaining)
end
function Base.close(io::_PhysicalCappedIO)
    io.closed && return nothing
    io.closed = true
    close(io.io)
    nothing
end
function Base.readbytes!(io::_PhysicalCappedIO, buffer::AbstractVector{UInt8}, requested::Integer=length(buffer))
    requested >= 0 || throw(ArgumentError("requested byte count must be nonnegative"))
    remaining = io.limit - io.bytes_read
    if remaining <= 0
        eof(io.io) && return 0
        throw(PhysicalStreamLimitError(:physical_source_bytes, io.limit, io.bytes_read + 1,
            0, PhysicalStreamCountersV1(0, io.bytes_read, 0), false))
    end
    count = readbytes!(io.io, buffer, min(Int64(requested), remaining))
    io.bytes_read += count
    count
end
function Base.read(io::_PhysicalCappedIO, ::Type{UInt8})
    remaining = io.limit - io.bytes_read
    if remaining <= 0
        eof(io.io) && throw(EOFError())
        throw(PhysicalStreamLimitError(:physical_source_bytes, io.limit, io.bytes_read + 1,
            0, PhysicalStreamCountersV1(0, io.bytes_read, 0), false))
    end
    value = read(io.io, UInt8)
    io.bytes_read += 1
    value
end

_read_ahead(io::BufferedReader) = max(Int64(io.buffer_size) - Int64(io.buffer_pos) + 1, Int64(0))
_counters(io::BufferedReader, source::_PhysicalCappedIO) =
    PhysicalStreamCountersV1(Int64(io.total_read), source.bytes_read, _read_ahead(io))
_limit(bound, limit, observed, ordinal, io, source) =
    PhysicalStreamLimitError(bound, Int64(limit), Int64(observed), ordinal, _counters(io, source), false)

function _bounded_read(io::BufferedReader, source::_PhysicalCappedIO, count::Integer,
                       caps::PhysicalStreamCapsV1, ordinal::UInt64)
    expected = Base.checked_add(Int64(io.total_read), Int64(count))
    expected <= caps.max_logical_bytes ||
        throw(_limit(:logical_bytes, caps.max_logical_bytes, expected, ordinal, io, source))
    read(io, Int(count))
end

function _le(::Type{T}, bytes::AbstractVector{UInt8}, first::Integer) where {T}
    first + sizeof(T) - 1 <= length(bytes) || return nothing
    ltoh(read(IOBuffer(bytes[first:first + sizeof(T) - 1]), T))
end
_raw_header(bytes) = PhysicalRecordHeaderV1(
    isempty(bytes) ? nothing : bytes[1], length(bytes) < 2 ? nothing : bytes[2],
    _le(UInt16, bytes, 3), _le(UInt32, bytes, 5), _le(Int64, bytes, 9))

function _event(ordinal, disposition, reason, header_bytes, body, record,
                logical_start, io, source)
    raw_rtype = length(header_bytes) >= 2 ? header_bytes[2] : nothing
    cmbp_layout = raw_rtype == UInt8(RType.CMBP_1_MSG) && length(body) >= 14
    PhysicalRecordEventV1(ordinal, disposition, reason, _raw_header(header_bytes),
        cmbp_layout ? body[13] : nothing, cmbp_layout ? body[14] : nothing, record,
        logical_start, Int64(io.total_read), _counters(io, source))
end

function _decode_event(ordinal, header_bytes, body, logical_start, io, source)
    raw = _raw_header(header_bytes)
    rtype = try RType.T(raw.rtype_byte::UInt8) catch; nothing end
    rtype === nothing && return _event(ordinal, :unknown, :unknown_rtype,
        header_bytes, body, nothing, logical_start, io, source)
    header = RecordHeader(raw.length_units::UInt8, rtype, raw.publisher_id::UInt16,
        raw.instrument_id::UInt32, raw.ts_event::Int64)
    if rtype == RType.CMBP_1_MSG
        Int(header.length) * 4 == 80 || return _event(ordinal, :malformed,
            :unexpected_cmbp1_length, header_bytes, body, nothing, logical_start, io, source)
        try
            decoder = DBNDecoder(IOBuffer(body))
            record = read_cmbp1_msg(decoder, header)
            eof(decoder.io) || return _event(ordinal, :malformed, :unconsumed_record_bytes,
                header_bytes, body, nothing, logical_start, io, source)
            return _event(ordinal, :cmbp1, :decoded, header_bytes, body, record,
                logical_start, io, source)
        catch
            return _event(ordinal, :malformed, :decode_failure,
                header_bytes, body, nothing, logical_start, io, source)
        end
    elseif rtype in (RType.ERROR_MSG, RType.SYSTEM_MSG, RType.SYMBOL_MAPPING_MSG)
        try
            decoder = DBNDecoder(IOBuffer(body))
            record = read_record_dispatch(decoder, header, rtype)
            record === nothing && return _event(ordinal, :malformed, :control_decode_failure,
                header_bytes, body, nothing, logical_start, io, source)
            eof(decoder.io) || return _event(ordinal, :malformed, :unconsumed_record_bytes,
                header_bytes, body, nothing, logical_start, io, source)
            return _event(ordinal, :control, :decoded, header_bytes, body, record,
                logical_start, io, source)
        catch
            return _event(ordinal, :malformed, :decode_failure,
                header_bytes, body, nothing, logical_start, io, source)
        end
    end
    _event(ordinal, :unknown, :unsupported_known_rtype,
        header_bytes, body, nothing, logical_start, io, source)
end

function _callback_decision(callback, event)
    decision = callback(event)
    decision isa PhysicalStreamDecisionV1 ||
        throw(ArgumentError("physical stream callback must return PhysicalStreamDecisionV1"))
    decision
end

"""
    foreach_physical_cmbp1(callback, path, caps) -> PhysicalStreamSummaryV1

Stream every physical record in a CMBP-1 DBN source without retaining the
input. Every complete record, including control and unsupported records, gets a
zero-based ordinal and one callback. Malformed records get one callback and
terminate. Physical source bytes are bytes pulled and may include compressed
read-ahead; decompressed buffered read-ahead is reported separately. The source
is closed on every return and exception.
"""
function foreach_physical_cmbp1(callback::F, path::AbstractString,
                                caps::PhysicalStreamCapsV1) where {F}
    base = open(path, "r")
    capped = nothing
    reader = nothing
    limit_failure = nothing
    version = UInt8(0); metadata_length = UInt32(0); schema_raw = UInt16(0)
    compressed = false; records_read = UInt64(0)
    counts = Dict{Symbol,UInt64}(:cmbp1 => 0, :control => 0, :unknown => 0, :malformed => 0)
    retained = nothing; terminal = :uninitialized; reached_eof = false
    counters = PhysicalStreamCountersV1(0, 0, 0)
    try
        magic = read(base, 4)
        length(magic) == 4 || throw(PhysicalStreamFormatError(:truncated_magic,
            "source is shorter than four bytes"))
        seekstart(base)
        capped = _PhysicalCappedIO(base, caps.max_physical_source_bytes, 4, false)
        compressed = magic == UInt8[0x28, 0xb5, 0x2f, 0xfd] || endswith(lowercase(path), ".zst")
        logical = compressed ? TranscodingStream(ZstdDecompressor(), capped) : capped
        reader = BufferedReader(logical)

        prefix = _bounded_read(reader, capped, 8, caps, records_read)
        length(prefix) == 8 || throw(PhysicalStreamFormatError(:truncated_metadata_prefix,
            "metadata prefix is incomplete"))
        prefix[1:3] == UInt8[0x44, 0x42, 0x4e] ||
            throw(PhysicalStreamFormatError(:invalid_magic, "expected DBN magic"))
        version = prefix[4]
        version in (UInt8(2), UInt8(3)) || throw(PhysicalStreamFormatError(
            :unsupported_version, "only DBN versions 2 and 3 are supported"))
        metadata_length = _le(UInt32, prefix, 5)::UInt32
        Int64(metadata_length) <= caps.max_metadata_bytes || throw(_limit(:metadata_bytes,
            caps.max_metadata_bytes, metadata_length, records_read, reader, capped))
        metadata = _bounded_read(reader, capped, metadata_length, caps, records_read)
        length(metadata) == metadata_length ||
            throw(PhysicalStreamFormatError(:truncated_metadata, "metadata payload is incomplete"))
        length(metadata) >= 18 ||
            throw(PhysicalStreamFormatError(:truncated_metadata, "metadata omits schema"))
        schema_raw = _le(UInt16, metadata, 17)::UInt16
        schema_raw == UInt16(Schema.CMBP_1) || throw(PhysicalStreamFormatError(
            :unexpected_schema, "physical streaming requires CMBP_1 metadata"))

        while true
            if eof(reader)
                terminal = :eof; reached_eof = true; break
            end
            records_read < caps.max_records || throw(_limit(:records,
                caps.max_records, records_read + 1, records_read, reader, capped))
            logical_start = Int64(reader.total_read)
            header_bytes = _bounded_read(reader, capped, 16, caps, records_read)
            if length(header_bytes) < 16
                event = _event(records_read, :malformed, :truncated_header, header_bytes,
                    UInt8[], nothing, logical_start, reader, capped)
                counts[:malformed] += 1; records_read += 1
                _callback_decision(callback, event); terminal = :malformed_record; break
            end
            declared_length = Int(header_bytes[1]) * 4
            if declared_length < 16
                event = _event(records_read, :malformed, :invalid_record_length, header_bytes,
                    UInt8[], nothing, logical_start, reader, capped)
                counts[:malformed] += 1; records_read += 1
                _callback_decision(callback, event); terminal = :malformed_record; break
            end
            body = _bounded_read(reader, capped, declared_length - 16, caps, records_read)
            if length(body) != declared_length - 16
                event = _event(records_read, :malformed, :truncated_body, header_bytes,
                    body, nothing, logical_start, reader, capped)
                counts[:malformed] += 1; records_read += 1
                _callback_decision(callback, event); terminal = :malformed_record; break
            end
            event = _decode_event(records_read, header_bytes, body, logical_start, reader, capped)
            counts[event.disposition] += 1; records_read += 1
            decision = _callback_decision(callback, event)
            if event.disposition == :malformed
                terminal = :malformed_record; break
            elseif decision == PHYSICAL_RETAIN_LOOKAHEAD
                retained = event; terminal = :retained_lookahead; break
            elseif decision == PHYSICAL_STOP
                terminal = :callback_stop; break
            end
        end
        counters = _counters(reader, capped)
    catch error
        if error isa PhysicalStreamLimitError
            error.ordinal = records_read
            reader === nothing || (error.counters = _counters(reader, capped))
            limit_failure = error
        end
        rethrow()
    finally
        try
            reader === nothing ? (capped === nothing ? close(base) : close(capped)) : close(reader)
        catch
            try close(base) catch end
        end
        isopen(base) && try close(base) catch end
        limit_failure === nothing || (limit_failure.source_closed = !isopen(base))
    end
    PhysicalStreamSummaryV1(version, metadata_length, schema_raw, compressed,
        records_read, counts, retained, terminal, reached_eof, !isopen(base), counters)
end
