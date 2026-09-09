# Wire layout references:
# - Rust: databento/dbn rust/dbn/src/record.rs, Cmbp1Msg and
#   ConsolidatedBidAskPair
# - Python: databento_dbn._lib.CMBP1Msg and ConsolidatedBidAskPair
# Both expose bid_pb/ask_pb and omit depth/sequence for CMBP-1/TCBBO.

using JSON3

@testset "CMBP-1/TCBBO authoritative consolidated layout" begin
    metadata(schema) = DBN.Metadata(
        UInt8(DBN.DBN_VERSION), "TEST.DATA", schema, Int64(1), Int64(2),
        UInt64(1), DBN.SType.RAW_SYMBOL, DBN.SType.RAW_SYMBOL, false,
        ["TEST"], String[], String[], Tuple{String,String,Int64,Int64}[],
    )

    function decode_body(reader, rtype)
        hd = DBN.RecordHeader(UInt8(20), rtype, UInt16(17), UInt32(23), Int64(31))
        body = IOBuffer()
        write(body, Int64(37))
        write(body, UInt32(41))
        write(body, UInt8(DBN.Action.MODIFY))
        write(body, UInt8(DBN.Side.BID))
        write(body, UInt8(0xa5))
        write(body, UInt8(0x6a))
        write(body, UInt64(43))
        write(body, Int32(47))
        write(body, UInt8[0x91, 0x92, 0x93, 0x94])
        write(body, Int64(59))
        write(body, Int64(61))
        write(body, UInt32(67))
        write(body, UInt32(71))
        write(body, UInt8[0x34, 0x12, 0x55, 0x66, 0x78, 0x56, 0x77, 0x88])
        raw_body = take!(body)
        @test length(raw_body) == 64
        return reader(DBN.DBNDecoder(IOBuffer(raw_body)), hd)
    end

    for (rtype, schema, reader, T) in (
        (DBN.RType.CMBP_1_MSG, DBN.Schema.CMBP_1, DBN.read_cmbp1_msg, DBN.CMBP1Msg),
        (DBN.RType.TCBBO_MSG, DBN.Schema.TCBBO, DBN.read_tcbbo_msg, DBN.TCBBOMsg),
    )
        rec = decode_body(reader, rtype)
        @test rec isa T
        @test rec._reserved1 == 0x6a
        @test rec.ts_recv == UInt64(43)
        @test rec._reserved2 == (0x91, 0x92, 0x93, 0x94)
        @test !hasproperty(rec, :depth)
        @test !hasproperty(rec, :sequence)
        @test rec.levels isa DBN.ConsolidatedBidAskPair
        @test rec.levels.bid_px == 59
        @test rec.levels.ask_px == 61
        @test rec.levels.bid_sz == 67
        @test rec.levels.ask_sz == 71
        @test rec.levels.bid_pb == 0x1234
        @test rec.levels._reserved1 == (0x55, 0x66)
        @test rec.levels.ask_pb == 0x5678
        @test rec.levels._reserved2 == (0x77, 0x88)

        encoded = IOBuffer()
        DBN.write_record(DBN.DBNEncoder(encoded, metadata(schema)), rec)
        raw_record = take!(encoded)
        @test length(raw_record) == 80
        @test raw_record[32] == 0x6a
        @test raw_record[45:48] == UInt8[0x91, 0x92, 0x93, 0x94]
        @test raw_record[73:80] == UInt8[0x34, 0x12, 0x55, 0x66, 0x78, 0x56, 0x77, 0x88]
        @test reader(DBN.DBNDecoder(IOBuffer(raw_record[17:end])), rec.hd) == rec

        public_dict = DBN.record_to_dict(rec)
        @test !haskey(public_dict, "_reserved1")
        @test !haskey(public_dict, "_reserved2")
        @test length(public_dict["levels"]) == 1
        @test Set(keys(only(public_dict["levels"]))) ==
            Set(["bid_px", "ask_px", "bid_sz", "ask_sz", "bid_pb", "ask_pb"])
        parsed = DBN.parse_json_record(public_dict)
        @test parsed._reserved1 == 0x00
        @test parsed._reserved2 == (0x00, 0x00, 0x00, 0x00)
        @test parsed.levels._reserved1 == (0x00, 0x00)
        @test parsed.levels._reserved2 == (0x00, 0x00)
        @test parsed.levels.bid_pb == rec.levels.bid_pb
        @test parsed.levels.ask_pb == rec.levels.ask_pb

        public_record = T(
            rec.hd, rec.price, rec.size, rec.action, rec.side, rec.flags,
            typemax(UInt64), rec.ts_in_delta,
            DBN.ConsolidatedBidAskPair(
                rec.levels.bid_px, rec.levels.ask_px, rec.levels.bid_sz,
                rec.levels.ask_sz, rec.levels.bid_pb, rec.levels.ask_pb,
            ),
        )
        json = JSON3.write(public_record)
        @test !occursin("_reserved", json)
        @test occursin("\"levels\":[", json)
        public_roundtrip = DBN.parse_json_record(DBN.record_to_dict(public_record))
        @test typeof(public_roundtrip) === T
        for field in fieldnames(T)
            if field === :hd
                for header_field in fieldnames(DBN.RecordHeader)
                    @test getfield(public_roundtrip.hd, header_field) ==
                        getfield(public_record.hd, header_field)
                end
            else
                @test getfield(public_roundtrip, field) == getfield(public_record, field)
            end
        end

        shown = sprint(show, rec)
        @test occursin("bid_pb=4660", shown)
        @test occursin("ask_pb=22136", shown)
        @test !occursin("bid_ct", shown)
    end
end
