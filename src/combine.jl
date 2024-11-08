"""
    combine_csvs(source; file_delim = "___", file_assign = "=", csv_parsing_args...)

Reads in a collection of CSV files and perform a join based on file name information. 

The `source` argument can be either of type `Vector{String}`, to provide a list of paths 
to CSVs, or a single `String` which is then interpreted as a directory from which all the 
`.csv` files in it will be read.

We assume the file names following the format:
```
key1=value11___key2=value21.csv 
key1=value12___key2=value22.csv 
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

Each CSV is read with `CSV.read(paths[i], DataFrame, csv_parsing_args...)`, while 
checking the column names match. We also check that the keys in file names are 
disjoint to the key used inside the CSVs. 

For each row in a given file, the key value pairs implied by its name are appended to the left.

The default choice of `___` and `=` can be overridden with `file_delim` and `file_assign` respectively. 
"""
function combine_csvs(source; file_delim = "___", file_assign = "=", csv_parsing_args...)
    paths = get_paths(source)
    @assert paths isa Vector "We need deterministic order on the paths."
    if isempty(paths)
        return DataFrame() 
    end
    index = index_data_frame(paths, file_delim, file_assign)
    result = nothing
    prev_names =  nothing
    for i in eachindex(paths)
        current = CSV.read(paths[i], DataFrame; csv_parsing_args...)
        if i == 1
            prev_names = names(current)
            @assert isdisjoint(names(index), prev_names) "Header and csv keys expected disjoint but got: $(names(index)), $(names(current))"
            result = vcat(similar(index, 0), similar(current, 0), cols = :union)
        else
            @assert names(current) == prev_names "All csv should have the same keys but got: $(names(current)), $prev_names"
        end
        index_row = Tuple(index[i, :])
        for r in eachrow(current)
            data_row = Tuple(r) 
            push!(result, (index_row..., data_row...))
        end
    end
    return result
end

function file_name(key_values; file_delim = "___", file_assign = "=")
    for (k, v) in pairs(key_values)
        @assert !contains(file_delim, string(k))
        @assert !contains(file_delim, string(v)) 
        @assert !contains(file_assign, string(k))
    end
    strings = map(kv -> "$(kv[1])$file_assign$(kv[2])", collect(pairs(key_values)))
    return join(strings, file_delim) * ".csv"
end

get_paths(source::Vector) = source 
get_paths(directory::String) = map(x -> "$directory/$x", filter(x -> endswith(x, ".csv"), readdir(directory)))

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
    str = remove_csv_suffix(str)
    return map(split(str, file_delim)) do entry 
        kv = split(entry, file_assign)
        @assert length(kv) ≥ 2 "Expected LHS$(file_delim)RHS but found: $entry"
        if length(kv) > 2 
            kv = (kv[1], join(kv[2:end], file_assign))
        end
        return kv
    end
end

function remove_csv_suffix(str)
    @assert endswith(str, ".csv") "key_value csv should end in .csv"
    return str[1:(end-4)]
end
