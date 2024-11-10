using CombineCSVs
using Test
using CSV 
using DataFrames

function combine_csvs_as_df(input; args...) 
    tmp = tempdir()
    combine_csvs(input, tmp; args...)
    return CSV.read("$tmp/file.csv", DataFrame)
end

@testset "Well formed examples" begin
    ref = """4×4 DataFrame
 Row │ a      b          first  second
     │ Int64  String15   Int64  Int64
─────┼─────────────────────────────────
   1 │     1  asf,asd        1       2
   2 │     1  asf,asd        3       4
   3 │     2  asf,asd 2      5       6
   4 │     2  asf,asd 2      7       8"""

    @test string(combine_csvs_as_df("example1")) == ref
    #@test string(combine_csvs_as_df(["example1/a=1___b=asf,asd.csv", "example1/a=2___b=asf,asd 2.csv"])) == ref

    #@test combine_csvs_as_df([]) == DataFrame()
end



@testset "Detection of ill formed examples" begin
    @test_throws "AssertionError: Inconsistent csv file keys: a___b vs a___c" combine_csvs_as_df("example2")
    @test_throws "AssertionError: Header and csv keys expected disjoint but got:" combine_csvs_as_df("example3")
    @test_throws "AssertionError: Expected LHS___RHS but found: a" combine_csvs_as_df("example6")
end

@testset "Edge case: = in value" begin
    ref = """4×4 DataFrame
 Row │ a      b          first  second
     │ Int64  String15   Int64  Int64
─────┼─────────────────────────────────
   1 │     1  asf,a=sd       1       2
   2 │     1  asf,a=sd       3       4
   3 │     2  asf,asd 2      5       6
   4 │     2  asf,asd 2      7       8"""

   @test string(combine_csvs_as_df("example4")) == ref
end

@testset "Weird delims" begin
    ref = """4×4 DataFrame
 Row │ a      b          first  second
     │ Int64  String15   Int64  Int64
─────┼─────────────────────────────────
   1 │     1  asf,a-sd       1       2
   2 │     1  asf,a-sd       3       4
   3 │     2  asf,asd 2      5       6
   4 │     2  asf,asd 2      7       8"""

   @test string(combine_csvs_as_df("example5"; file_delim = "__", file_assign = "-")) == ref
end

@testset "CSV parsing opts" begin
    
    ref = """4×4 DataFrame
 Row │ a      b          first  second
     │ Int64  String15   Int64  Int64
─────┼─────────────────────────────────
   1 │     1  asf,asd        1       2
   2 │     1  asf,asd        3       4
   3 │     2  asf,asd 2      5       6
   4 │     2  asf,asd 2      7       8"""
   
       @test string(combine_csvs_as_df("example7"; comment = "#")) == ref

end


@testset "Multiple outs" begin
    ref = """4×4 DataFrame
 Row │ a      b          first  second
     │ Int64  String15   Int64  Int64
─────┼─────────────────────────────────
   1 │     1  asf,asd        1       2
   2 │     1  asf,asd        3       4
   3 │     2  asf,asd 2      5       6
   4 │     2  asf,asd 2      7       8"""

    @test string(combine_csvs_as_df("example8")) == ref

    tmp = tempdir()
    combine_csvs("example8", tmp)
    df2 = CSV.read("$tmp/file2.csv", DataFrame)

    ref2 = """4×3 DataFrame
 Row │ a      b        anothercol
     │ Int64  String7  Int64
─────┼────────────────────────────
   1 │    10  asf,asd        1000
   2 │    10  asf,asd        1001
   3 │     1  asf,asd        1000
   4 │     1  asf,asd        1001"""

    @test string(df2) == ref2

end


@testset "Weird csv" begin
    ref = """4×4 DataFrame
 Row │ a      b          first  second
     │ Int64  String15   Int64  Int64
─────┼─────────────────────────────────
   1 │     1  asf,asd        1       2
   2 │     1  asf,asd        3       4
   3 │     2  asf,asd 2      5       6
   4 │     2  asf,asd 2      7       8"""

    df = combine_csvs_as_df("example9", comment = "#")

    @test nrow(df) == 4
    @test replace(df[1,"second"], "\r" => "") == """2
    blah"""

end

