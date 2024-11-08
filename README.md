## Usage
    
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

For more information use `?combine_csvs` or [see the doc](https://github.com/UBC-Stat-ML/CombineCSVs/blob/main/src/combine.jl).

## Install

```
using Pkg
Pkg.add("https://github.com/UBC-Stat-ML/CombineCSVs.git")
```
