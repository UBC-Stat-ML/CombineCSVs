"""
    combine_csvs(input_dir, output_dir; file_delim = "___", file_assign = "=", csv_parsing_args...)

Reads in a collection of CSV files and perform a join based on file name information. 

The `input_dir` argument is the path to a directory from which all the 
`.csv` files in it will be read. The `output_dir` is a directory where the output will be written to.

We assume the file location and names conform the following format:
```
input_dir/key1=value11___key2=value21/my_csv.csv 
input_dir/key1=value12___key2=value22/my_csv.csv
...
```
where we assume `keyi` does not contain the string `=` and that
neither `keyi` nor `valueij` contain the string `___`.

These will be internally transformed into an in-memory index CSV of the format
```
key1___key2 
value11___value21 
value12___value22
``` 
which is parsed via `CSV.read(...; delim = "___")``. 

The CSVs are read with `CSV.read(paths[i], DataFrame, csv_parsing_args...)`, while 
checking the column names match. We also check that the keys in file names are 
disjoint to the key used inside the CSVs. 

For each row in a given file, the key value pairs implied by its name are appended to the left.
The output is written to output_dir/my_csv.csv

If the input directories contain not just my_csv.csv but my_csv1.csv, my_csv2.csv, etc, the process 
is repeated. We do not assume my_csv1.csv and my_csv2.csv have the same columns. 

The default choice of `___` and `=` can be overridden with `file_delim` and `file_assign` respectively. 
"""
function combine_csvs(input_dir, output_dir; file_delim = "___", file_assign = "=", csv_parsing_args...)
    mkpath(output_dir)
    plan = organize_paths(input_dir, file_assign)
    
    for key in keys(plan) 
        paths = plan[key]

        index = index_data_frame(paths, file_delim, file_assign)
        result = nothing
        prev_names =  nothing
        for i in eachindex(paths)
            current = CSV.read("$input_dir/$(paths[i])/$key", DataFrame; limit = 1, types = String, csv_parsing_args...)
            if i == 1
                prev_names = names(current)
                @assert isdisjoint(names(index), prev_names) "Header and csv keys expected disjoint but got: $(names(index)), $(names(current))"
                result = vcat(similar(index, 0), similar(current, 0), cols = :union)
            else
                @assert names(current) == prev_names "All csv should have the same keys but got: $(names(current)), $prev_names"
            end
            index_row = Tuple(index[i, :])
            for r in CSV.Rows("$input_dir/$(paths[i])/$key"; csv_parsing_args...)
                data_row = Tuple(r) 
                push!(result, (index_row..., data_row...))
            end
        end

        CSV.write("$output_dir/$key", result)
    end

    return nothing
end

function organize_paths(directory::String, file_assign) 
    result = Dict{String, Vector{String}}() # name.csv -> ["key1=value11___key2=value12", "key1=value22___key2=value22", ...]
    for sub_dir_name in filter(x -> contains(x, file_assign), readdir(directory))
        cur_sub_dir = "$directory/$sub_dir_name" 
        for csv_file in filter(x -> endswith(x, ".csv"), readdir(cur_sub_dir)) 
            list = get_or_push!(result, csv_file)
            push!(list, sub_dir_name)
        end
    end
    return result
end

function get_or_push!(dict, key) 
    if haskey(dict, key) 
        return dict[key]
    end
    result = Vector{String}()
    dict[key] = result 
    return result
end

function index_data_frame(paths::Vector{String}, file_delim::String, file_assign::String)
    ref = parse_key_value_csv(paths[1], file_delim, file_assign)
    ref_header = process(ref, true, file_delim)
    temp_csv_str = ref_header
    for path in paths 
        entry = parse_key_value_csv(path, file_delim, file_assign)
        keys = process(entry, true, file_delim)
        @assert ref_header == keys "Inconsistent csv file keys: $ref_header vs $keys"
        temp_csv_str = temp_csv_str * "\n" * process(entry, false, file_delim)
    end
    CSV.read(IOBuffer(temp_csv_str), DataFrame; delim = file_delim)
end

function process(ref, is_header::Bool, file_delim::String) 
    items = map(is_header ? first : last, ref)
    return join(items, file_delim)
end

function parse_key_value_csv(path::String, file_delim::String, file_assign::String)
    str = basename(path)
    return map(split(str, file_delim)) do entry 
        kv = split(entry, file_assign)
        @assert length(kv) ≥ 2 "Expected LHS$(file_delim)RHS but found: $entry"
        if length(kv) > 2 
            kv = (kv[1], join(kv[2:end], file_assign))
        end
        return kv
    end
end
