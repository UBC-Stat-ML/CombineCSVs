using CombineCSVs
using Test
using CSV 
using DataFrames

@testset "Well formed examples" begin
    ref = """4×4 DataFrame
 Row │ a       b          first   second
     │ Int64?  String15?  Int64?  Int64?
─────┼───────────────────────────────────
   1 │      1  asf,asd         1       2
   2 │      1  asf,asd         3       4
   3 │      2  asf,asd 2       5       6
   4 │      2  asf,asd 2       7       8"""

    @test string(combine_csvs("example1")) == ref
    @test string(combine_csvs(["example1/a=1___b=asf,asd.csv", "example1/a=2___b=asf,asd 2.csv"])) == ref

    @test combine_csvs([]) == DataFrame()
end



@testset "Detection of ill formed examples" begin
    @test_throws "AssertionError: Inconsistent csv file keys: a___b vs a___c" combine_csvs("example2")
    @test_throws "AssertionError: Header and csv keys expected disjoint but got:" combine_csvs("example3")
    @test_throws "AssertionError: Expected LHS___RHS but found: a" combine_csvs("example6")
end

@testset "Edge case: = in value" begin
    ref = """4×4 DataFrame
 Row │ a       b          first   second
     │ Int64?  String15?  Int64?  Int64?
─────┼───────────────────────────────────
   1 │      1  asf,a=sd        1       2
   2 │      1  asf,a=sd        3       4
   3 │      2  asf,asd 2       5       6
   4 │      2  asf,asd 2       7       8"""

   @test string(combine_csvs("example4")) == ref
end

@testset "Weird delims" begin
    ref = """4×4 DataFrame
 Row │ a       b          first   second
     │ Int64?  String15?  Int64?  Int64?
─────┼───────────────────────────────────
   1 │      1  asf,a-sd        1       2
   2 │      1  asf,a-sd        3       4
   3 │      2  asf,asd 2       5       6
   4 │      2  asf,asd 2       7       8"""

   @test string(combine_csvs("example5"; file_delim = "__", file_assign = "-")) == ref
end
