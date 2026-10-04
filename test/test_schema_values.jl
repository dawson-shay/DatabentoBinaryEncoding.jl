# Regression: Schema discriminants must match DBN (rust/dbn/src/enums.rs).
# Values 13-16 were previously CBBO/CBBO_1S/CBBO_1M/CMBP_1, so cmbp-1 metadata
# decoded as CBBO_1S and files written as CMBP_1 were read by other DBN tools as cbbo-1m.
@testset "Schema discriminants match DBN" begin
    expected = [
        Schema.MBO => 0, Schema.MBP_1 => 1, Schema.MBP_10 => 2, Schema.TBBO => 3,
        Schema.TRADES => 4, Schema.OHLCV_1S => 5, Schema.OHLCV_1M => 6,
        Schema.OHLCV_1H => 7, Schema.OHLCV_1D => 8, Schema.DEFINITION => 9,
        Schema.STATISTICS => 10, Schema.STATUS => 11, Schema.IMBALANCE => 12,
        Schema.OHLCV_EOD => 13, Schema.CMBP_1 => 14, Schema.CBBO_1S => 15,
        Schema.CBBO_1M => 16, Schema.TCBBO => 17, Schema.BBO_1S => 18, Schema.BBO_1M => 19,
    ]
    for (schema, value) in expected
        @test UInt16(schema) == value
    end

    # The schema is written as a little-endian u16 after the 8-byte prefix and
    # 16-byte dataset, and must survive a round trip
    metadata = Metadata(
        UInt8(DBN_VERSION), "TEST.DATA", Schema.CMBP_1, 1640995200000000000,
        1640995260000000000, UInt64(0), SType.RAW_SYMBOL, SType.INSTRUMENT_ID,
        false, String[], String[], String[], Tuple{String,String,Int64,Int64}[],
    )
    path = tempname() * ".dbn"
    try
        write_dbn(path, metadata, CMBP1Msg[])
        bytes = read(path)
        @test bytes[25] == 14 && bytes[26] == 0
        @test read_dbn_with_metadata(path)[1].schema == Schema.CMBP_1
    finally
        rm(path; force=true)
    end
end
