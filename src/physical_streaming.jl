"""Callback decision for `foreach_physical_cmbp1`."""
@enum PhysicalStreamDecisionV1::UInt8 begin
    PHYSICAL_CONTINUE = 0
    PHYSICAL_RETAIN_LOOKAHEAD = 1
    PHYSICAL_STOP = 2
end

const _DEFAULT_OUTER_BUFFER_CAPACITY = 2 * 64 * 1024
const _DEFAULT_TRANSCODER_BUFFER_CAPACITY = 2 * 16 * 1024

struct PhysicalStreamCapsV1
    max_metadata_bytes::Int64
    max_logical_bytes::Int64
    max_physical_source_bytes::Int64
    max_records::UInt64
    max_outer_buffer_capacity_bytes::Int64
    max_transcoder_buffer_capacity_bytes::Int64

    function PhysicalStreamCapsV1(metadata::Integer, logical::Integer, physical::Integer,
                                  records::Integer, outer::Integer, transcoder::Integer)
        metadata > 0 || throw(ArgumentError("max_metadata_bytes must be positive"))
        logical > 0 || throw(ArgumentError("max_logical_bytes must be positive"))
        physical >= 4 || throw(ArgumentError("max_physical_source_bytes must be at least four"))
        records > 0 || throw(ArgumentError("max_records must be positive"))
        outer > 0 || throw(ArgumentError("max_outer_buffer_capacity_bytes must be positive"))
        transcoder >= 0 || throw(ArgumentError("max_transcoder_buffer_capacity_bytes must be nonnegative"))
        new(Int64(metadata), Int64(logical), Int64(physical), UInt64(records),
            Int64(outer), Int64(transcoder))
    end
end

PhysicalStreamCapsV1(metadata::Integer, logical::Integer, physical::Integer, records::Integer) =
    PhysicalStreamCapsV1(metadata, logical, physical, records,
        _DEFAULT_OUTER_BUFFER_CAPACITY, _DEFAULT_TRANSCODER_BUFFER_CAPACITY)
PhysicalStreamCapsV1(; max_metadata_bytes, max_logical_bytes, max_physical_source_bytes,
    max_records, max_outer_buffer_capacity_bytes=_DEFAULT_OUTER_BUFFER_CAPACITY,
    max_transcoder_buffer_capacity_bytes=_DEFAULT_TRANSCODER_BUFFER_CAPACITY) =
    PhysicalStreamCapsV1(max_metadata_bytes, max_logical_bytes, max_physical_source_bytes,
        max_records, max_outer_buffer_capacity_bytes, max_transcoder_buffer_capacity_bytes)

struct PhysicalStreamCountersV1
    logical_bytes_consumed::Int64
    physical_source_bytes_pulled::Int64
    outer_buffer_unread_bytes::Int64
    outer_buffer_capacity_bytes::Int64
    transcoder_exposed_unread_bytes::Int64
    transcoder_buffer_capacity_bytes::Int64
end
const _ZERO_COUNTERS = PhysicalStreamCountersV1(0, 0, 0, 0, 0, 0)

function _valid_sha256(value::AbstractString)
    length(value) == 64 && lowercase(value) == value && all(isxdigit, value)
end

struct PhysicalMetadataBindingV1
    header_metadata_sha256::String
    dataset::String
    schema_raw::UInt16
    stype_in_raw::Union{UInt8,Nothing}
    stype_out_raw::UInt8
    start_ts::Int64
    end_ts::Union{Int64,Nothing}
    mapping_count::UInt64
    mappings_sha256::String

    function PhysicalMetadataBindingV1(header_hash, dataset, schema, stype_in, stype_out,
                                       start_ts, end_ts, mapping_count, mappings_hash)
        _valid_sha256(header_hash) || throw(ArgumentError("invalid header_metadata_sha256"))
        _valid_sha256(mappings_hash) || throw(ArgumentError("invalid mappings_sha256"))
        mapping_count >= 0 || throw(ArgumentError("mapping_count must be nonnegative"))
        new(String(header_hash), String(dataset), UInt16(schema),
            stype_in === nothing ? nothing : UInt8(stype_in), UInt8(stype_out),
            Int64(start_ts), end_ts === nothing ? nothing : Int64(end_ts),
            UInt64(mapping_count), String(mappings_hash))
    end
end
PhysicalMetadataBindingV1(; header_metadata_sha256, dataset, schema_raw, stype_in_raw,
    stype_out_raw, start_ts, end_ts, mapping_count, mappings_sha256) =
    PhysicalMetadataBindingV1(header_metadata_sha256, dataset, schema_raw, stype_in_raw,
        stype_out_raw, start_ts, end_ts, mapping_count, mappings_sha256)

struct PhysicalMetadataIdentityV1
    header_metadata_bytes::Int64
    header_metadata_sha256::String
    version::UInt8
    dataset::String
    schema_raw::UInt16
    stype_in_raw::Union{UInt8,Nothing}
    stype_out_raw::UInt8
    start_ts::Int64
    end_ts::Union{Int64,Nothing}
    limit::Union{UInt64,Nothing}
    ts_out::Bool
    symbol_count::UInt64
    partial_count::UInt64
    not_found_count::UInt64
    mapping_count::UInt64
    mappings_sha256::String
end

function _write_identity_string(io::IO, value::AbstractString)
    bytes = codeunits(value)
    write(io, htol(UInt32(length(bytes))))
    write(io, bytes)
end

"""Hash mappings using count plus length-prefixed UTF-8 strings and little-endian Int64 times."""
function physical_mappings_sha256(mappings)
    io = IOBuffer()
    write(io, htol(UInt64(length(mappings))))
    for mapping in mappings
        _write_identity_string(io, mapping[1])
        _write_identity_string(io, mapping[2])
        write(io, htol(Int64(mapping[3])))
        write(io, htol(Int64(mapping[4])))
    end
    bytes2hex(SHA.sha256(take!(io)))
end

function _metadata_identity(raw::Vector{UInt8}, metadata::Metadata)
    PhysicalMetadataIdentityV1(length(raw), bytes2hex(SHA.sha256(raw)), metadata.version,
        metadata.dataset, UInt16(metadata.schema),
        metadata.stype_in === nothing ? nothing : UInt8(metadata.stype_in),
        UInt8(metadata.stype_out), metadata.start_ts, metadata.end_ts, metadata.limit,
        metadata.ts_out, length(metadata.symbols), length(metadata.partial),
        length(metadata.not_found), length(metadata.mappings),
        physical_mappings_sha256(metadata.mappings))
end

PhysicalMetadataBindingV1(identity::PhysicalMetadataIdentityV1) = PhysicalMetadataBindingV1(
    identity.header_metadata_sha256, identity.dataset, identity.schema_raw,
    identity.stype_in_raw, identity.stype_out_raw, identity.start_ts, identity.end_ts,
    identity.mapping_count, identity.mappings_sha256)

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
    declared_length_bytes::Union{Int64,Nothing}
    raw_record_bytes_count::Int64
    raw_record_sha256::String
    raw_action_byte::Union{UInt8,Nothing}
    raw_side_byte::Union{UInt8,Nothing}
    record::Union{DBNRecord,Nothing}
    logical_start_offset::Int64
    logical_end_offset::Int64
    counters::PhysicalStreamCountersV1
end

struct PhysicalStreamSummaryV1
    metadata::PhysicalMetadataIdentityV1
    compressed::Bool
    records_read::UInt64
    disposition_counts::Dict{Symbol,UInt64}
    retained_lookahead::Union{PhysicalRecordEventV1,Nothing}
    terminal_reason::Symbol
    eof_reached::Bool
    source_closed::Bool
    counters::PhysicalStreamCountersV1
end

mutable struct PhysicalStreamTerminalErrorV1 <: Exception
    kind::Symbol
    reason::Symbol
    detail::String
    ordinal::UInt64
    partial_bytes_count::Int64
    partial_bytes_sha256::Union{String,Nothing}
    counters::PhysicalStreamCountersV1
    source_closed::Bool
    bound::Union{Symbol,Nothing}
    limit::Union{Int64,Nothing}
    observed::Union{Int64,Nothing}
    cause_type::Union{String,Nothing}
end
const PhysicalStreamLimitError = PhysicalStreamTerminalErrorV1

function Base.showerror(io::IO, error::PhysicalStreamTerminalErrorV1)
    print(io, "physical stream ", error.kind, "/", error.reason, " at ordinal ",
        error.ordinal, ": ", error.detail)
end

function _failure(kind, reason, detail; ordinal=UInt64(0), partial=UInt8[], counters=_ZERO_COUNTERS,
                  partial_count=nothing, partial_sha256=nothing,
                  bound=nothing, limit=nothing, observed=nothing, cause=nothing)
    count = partial_count === nothing ? length(partial) : Int64(partial_count)
    digest = partial_sha256 === nothing ?
        (isempty(partial) ? nothing : bytes2hex(SHA.sha256(partial))) : String(partial_sha256)
    PhysicalStreamTerminalErrorV1(kind, reason, first(String(detail), min(length(String(detail)), 512)),
        ordinal, count, digest,
        counters, false, bound, limit === nothing ? nothing : Int64(limit),
        observed === nothing ? nothing : Int64(observed),
        cause === nothing ? nothing : string(typeof(cause)))
end

mutable struct _PhysicalCappedIO{T<:IO} <: IO
    io::T
    limit::Int64
    bytes_read::Int64
    closed::Bool
end
Base.isopen(io::_PhysicalCappedIO) = !io.closed && isopen(io.io)
Base.eof(io::_PhysicalCappedIO) = eof(io.io)
Base.position(io::_PhysicalCappedIO) = io.bytes_read
function Base.close(io::_PhysicalCappedIO)
    io.closed && return nothing
    io.closed = true
    close(io.io)
    nothing
end
function _physical_cap_failure(io::_PhysicalCappedIO)
    _failure(:resource, :physical_source_bytes_exceeded, "physical source byte cap exceeded";
        bound=:physical_source_bytes, limit=io.limit, observed=io.bytes_read + 1)
end
function Base.bytesavailable(io::_PhysicalCappedIO)
    remaining = io.limit - io.bytes_read
    remaining <= 0 && return eof(io.io) ? 0 : throw(_physical_cap_failure(io))
    min(bytesavailable(io.io), remaining)
end
function Base.readbytes!(io::_PhysicalCappedIO, buffer::AbstractVector{UInt8}, requested::Integer=length(buffer))
    requested >= 0 || throw(ArgumentError("requested byte count must be nonnegative"))
    remaining = io.limit - io.bytes_read
    remaining <= 0 && return eof(io.io) ? 0 : throw(_physical_cap_failure(io))
    count = readbytes!(io.io, buffer, min(Int64(requested), remaining))
    io.bytes_read += count
    count
end
function Base.read(io::_PhysicalCappedIO, ::Type{UInt8})
    io.limit > io.bytes_read || (eof(io.io) ? throw(EOFError()) : throw(_physical_cap_failure(io)))
    value = read(io.io, UInt8)
    io.bytes_read += 1
    value
end

function _transcoder_metrics(io)
    io isa TranscodingStream || return (Int64(0), Int64(0))
    buffers = (io.buffer1, io.buffer2)
    unread = sum(max(Int64(buffer.marginpos) - Int64(buffer.bufferpos), 0) for buffer in buffers)
    capacity = sum(Int64(length(buffer.data)) for buffer in buffers)
    (unread, capacity)
end

function _counters(reader::BufferedReader, source::_PhysicalCappedIO)
    unread = max(Int64(reader.buffer_size) - Int64(reader.buffer_pos) + 1, 0)
    outer_capacity = Int64(length(reader.buffer) + length(reader.refill_tmp))
    transcoder_unread, transcoder_capacity = _transcoder_metrics(reader.io)
    PhysicalStreamCountersV1(Int64(reader.total_read), source.bytes_read, unread,
        outer_capacity, transcoder_unread, transcoder_capacity)
end

function _check_buffer_caps(reader, source, caps, ordinal)
    counters = _counters(reader, source)
    counters.outer_buffer_capacity_bytes <= caps.max_outer_buffer_capacity_bytes ||
        throw(_failure(:resource, :outer_buffer_capacity_exceeded,
            "outer BufferedReader allocation exceeds cap"; ordinal, counters,
            bound=:outer_buffer_capacity_bytes, limit=caps.max_outer_buffer_capacity_bytes,
            observed=counters.outer_buffer_capacity_bytes))
    counters.transcoder_buffer_capacity_bytes <= caps.max_transcoder_buffer_capacity_bytes ||
        throw(_failure(:resource, :transcoder_buffer_capacity_exceeded,
            "exposed TranscodingStreams buffer allocation exceeds cap; opaque codec state is not included";
            ordinal, counters, bound=:transcoder_buffer_capacity_bytes,
            limit=caps.max_transcoder_buffer_capacity_bytes,
            observed=counters.transcoder_buffer_capacity_bytes))
    counters
end

function _bounded_read(reader, source, count, caps, ordinal)
    expected = Base.checked_add(Int64(reader.total_read), Int64(count))
    expected <= caps.max_logical_bytes || throw(_failure(:resource, :logical_bytes_exceeded,
        "logical byte cap exceeded"; ordinal, counters=_counters(reader, source),
        bound=:logical_bytes, limit=caps.max_logical_bytes, observed=expected))
    bytes = read(reader, Int(count))
    _check_buffer_caps(reader, source, caps, ordinal)
    bytes
end

function _le(::Type{T}, bytes::AbstractVector{UInt8}, first::Integer) where {T}
    first + sizeof(T) - 1 <= length(bytes) || return nothing
    ltoh(read(IOBuffer(bytes[first:first + sizeof(T) - 1]), T))
end
_raw_header(bytes) = PhysicalRecordHeaderV1(
    isempty(bytes) ? nothing : bytes[1], length(bytes) < 2 ? nothing : bytes[2],
    _le(UInt16, bytes, 3), _le(UInt32, bytes, 5), _le(Int64, bytes, 9))

function _raw_record_identity(header_bytes, body)
    bytes = vcat(header_bytes, body)
    length(bytes), bytes2hex(SHA.sha256(bytes))
end

function _event(ordinal, disposition, reason, header_bytes, body, record,
                logical_start, reader, source)
    raw = _raw_header(header_bytes)
    count, digest = _raw_record_identity(header_bytes, body)
    declared = raw.length_units === nothing ? nothing : Int64(raw.length_units) * 4
    cmbp_layout = raw.rtype_byte == UInt8(RType.CMBP_1_MSG) && length(body) >= 14
    PhysicalRecordEventV1(ordinal, disposition, reason, raw, declared, count, digest,
        cmbp_layout ? body[13] : nothing, cmbp_layout ? body[14] : nothing, record,
        logical_start, Int64(reader.total_read), _counters(reader, source))
end

function _decode_event(ordinal, header_bytes, body, logical_start, reader, source)
    raw = _raw_header(header_bytes)
    rtype = try RType.T(raw.rtype_byte::UInt8) catch; nothing end
    rtype === nothing && return _event(ordinal, :unknown, :unknown_rtype,
        header_bytes, body, nothing, logical_start, reader, source)
    header = RecordHeader(raw.length_units::UInt8, rtype, raw.publisher_id::UInt16,
        raw.instrument_id::UInt32, raw.ts_event::Int64)
    if rtype == RType.CMBP_1_MSG
        Int(header.length) * 4 == 80 || return _event(ordinal, :malformed,
            :unexpected_cmbp1_length, header_bytes, body, nothing, logical_start, reader, source)
        try
            decoder = DBNDecoder(IOBuffer(body)); record = read_cmbp1_msg(decoder, header)
            eof(decoder.io) || return _event(ordinal, :malformed, :unconsumed_record_bytes,
                header_bytes, body, nothing, logical_start, reader, source)
            return _event(ordinal, :cmbp1, :decoded, header_bytes, body, record,
                logical_start, reader, source)
        catch
            return _event(ordinal, :malformed, :decode_failure,
                header_bytes, body, nothing, logical_start, reader, source)
        end
    elseif rtype in (RType.ERROR_MSG, RType.SYSTEM_MSG, RType.SYMBOL_MAPPING_MSG)
        try
            decoder = DBNDecoder(IOBuffer(body)); record = read_record_dispatch(decoder, header, rtype)
            record === nothing && return _event(ordinal, :malformed, :control_decode_failure,
                header_bytes, body, nothing, logical_start, reader, source)
            eof(decoder.io) || return _event(ordinal, :malformed, :unconsumed_record_bytes,
                header_bytes, body, nothing, logical_start, reader, source)
            return _event(ordinal, :control, :decoded, header_bytes, body, record,
                logical_start, reader, source)
        catch
            return _event(ordinal, :malformed, :decode_failure,
                header_bytes, body, nothing, logical_start, reader, source)
        end
    end
    _event(ordinal, :unknown, :unsupported_known_rtype,
        header_bytes, body, nothing, logical_start, reader, source)
end

function _binding_mismatch(identity, expected)
    fields = (:header_metadata_sha256, :dataset, :schema_raw, :stype_in_raw,
        :stype_out_raw, :start_ts, :end_ts, :mapping_count, :mappings_sha256)
    for field in fields
        getfield(identity, field) == getfield(expected, field) || return field
    end
    nothing
end

function _callback_decision(callback, event)
    decision = try
        callback(event)
    catch error
        error isa PhysicalStreamTerminalErrorV1 && rethrow()
        throw(_failure(:callback, :callback_threw, sprint(showerror, error);
            ordinal=event.ordinal, partial_count=event.raw_record_bytes_count,
            partial_sha256=event.raw_record_sha256, counters=event.counters, cause=error))
    end
    decision isa PhysicalStreamDecisionV1 || throw(_failure(:invalid_decision,
        :invalid_callback_decision, "callback must return PhysicalStreamDecisionV1";
        ordinal=event.ordinal, partial_count=event.raw_record_bytes_count,
        partial_sha256=event.raw_record_sha256, counters=event.counters))
    decision
end

"""
    foreach_physical_cmbp1(callback, path, caps, expected_metadata)

Stream every physical record in a CMBP-1 source. Metadata identity is checked
before callbacks. Physical source bytes include format-probe and decompressor
read-ahead. Outer-buffer unread/capacity and TranscodingStreams' exposed buffer
unread/capacity are reported separately; opaque codec-internal state is not
included. Every source is closed before return or typed terminal failure.
"""
function foreach_physical_cmbp1(callback::F, path::AbstractString, caps::PhysicalStreamCapsV1,
                                expected::PhysicalMetadataBindingV1) where {F}
    base = open(path, "r"); capped = nothing; reader = nothing; failure = nothing
    metadata_identity = nothing; compressed = false; records_read = UInt64(0)
    counts = Dict{Symbol,UInt64}(:cmbp1 => 0, :control => 0, :unknown => 0, :malformed => 0)
    retained = nothing; terminal = :uninitialized; reached_eof = false
    final_counters = _ZERO_COUNTERS
    try
        magic = read(base, 4)
        length(magic) == 4 || throw(_failure(:format, :truncated_magic,
            "source is shorter than four bytes"; partial=magic))
        seekstart(base)
        capped = _PhysicalCappedIO(base, caps.max_physical_source_bytes, 4, false)
        compressed = magic == UInt8[0x28, 0xb5, 0x2f, 0xfd] || endswith(lowercase(path), ".zst")
        reader = BufferedReader(compressed ? TranscodingStream(ZstdDecompressor(), capped) : capped)
        _check_buffer_caps(reader, capped, caps, records_read)

        prefix = _bounded_read(reader, capped, 8, caps, records_read)
        length(prefix) == 8 || throw(_failure(:format, :truncated_metadata_prefix,
            "metadata prefix is incomplete"; partial=prefix, counters=_counters(reader, capped)))
        prefix[1:3] == UInt8[0x44, 0x42, 0x4e] || throw(_failure(:format,
            :invalid_magic, "expected DBN magic"; partial=prefix, counters=_counters(reader, capped)))
        prefix[4] in (UInt8(2), UInt8(3)) || throw(_failure(:format,
            :unsupported_version, "only DBN versions 2 and 3 are supported";
            partial=prefix, counters=_counters(reader, capped)))
        metadata_length = _le(UInt32, prefix, 5)::UInt32
        Int64(metadata_length) <= caps.max_metadata_bytes || throw(_failure(:resource,
            :metadata_bytes_exceeded, "metadata byte cap exceeded"; partial=prefix,
            counters=_counters(reader, capped), bound=:metadata_bytes,
            limit=caps.max_metadata_bytes, observed=metadata_length))
        metadata_bytes = _bounded_read(reader, capped, metadata_length, caps, records_read)
        raw_metadata = vcat(prefix, metadata_bytes)
        length(metadata_bytes) == metadata_length || throw(_failure(:format,
            :truncated_metadata, "metadata payload is incomplete"; partial=raw_metadata,
            counters=_counters(reader, capped)))
        header = try
            read_header!(DBNDecoder(IOBuffer(raw_metadata)))
        catch error
            throw(_failure(:format, :metadata_decode_failure, sprint(showerror, error);
                partial=raw_metadata, counters=_counters(reader, capped), cause=error))
        end
        metadata_identity = _metadata_identity(raw_metadata, header.metadata)
        metadata_identity.schema_raw == UInt16(Schema.CMBP_1) || throw(_failure(
            :format, :unexpected_schema, "physical streaming requires CMBP_1 metadata";
            partial=raw_metadata, counters=_counters(reader, capped)))
        mismatch = _binding_mismatch(metadata_identity, expected)
        mismatch === nothing || throw(_failure(:metadata_binding, Symbol("mismatch_", mismatch),
            "metadata identity does not match expected binding"; partial=raw_metadata,
            counters=_counters(reader, capped)))

        while true
            if eof(reader)
                _check_buffer_caps(reader, capped, caps, records_read)
                terminal = :eof; reached_eof = true; break
            end
            records_read < caps.max_records || throw(_failure(:resource, :records_exceeded,
                "record cap exceeded"; ordinal=records_read, counters=_counters(reader, capped),
                bound=:records, limit=caps.max_records, observed=records_read + 1))
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
        final_counters = _check_buffer_caps(reader, capped, caps, records_read)
    catch error
        failure = error isa PhysicalStreamTerminalErrorV1 ? error : _failure(:internal,
            :unexpected_exception, sprint(showerror, error); ordinal=records_read,
            counters=reader === nothing ? _ZERO_COUNTERS : _counters(reader, capped), cause=error)
    finally
        close_error = nothing
        try
            reader === nothing ? (capped === nothing ? close(base) : close(capped)) : close(reader)
        catch error
            close_error = error
            try close(base) catch end
        end
        isopen(base) && try close(base) catch error; close_error = error end
        closed = !isopen(base)
        if failure !== nothing
            failure.ordinal = records_read
            reader === nothing || (failure.counters = _counters(reader, capped))
            failure.source_closed = closed
        elseif close_error !== nothing || !closed
            failure = _failure(:cleanup, :source_close_failure,
                close_error === nothing ? "source remained open" : sprint(showerror, close_error);
                ordinal=records_read, counters=final_counters, cause=close_error)
            failure.source_closed = closed
        end
    end
    failure === nothing || throw(failure)
    PhysicalStreamSummaryV1(metadata_identity::PhysicalMetadataIdentityV1, compressed,
        records_read, counts, retained, terminal, reached_eof, true, final_counters)
end
