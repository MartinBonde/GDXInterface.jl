# High-level GDX file API for GDXInterface.jl
# User-friendly interface for reading and writing GDX files

# requires `import DataFrames`

# =============================================================================
# Symbol types
# =============================================================================

abstract type GDXSymbol end

"""
    GDXSet

A GAMS set with its elements and optional explanatory text.
"""
struct GDXSet <: GDXSymbol
    name::String
    description::String
    domain::Vector{String}
    records::DataFrames.DataFrame
end

"""
    GDXParameter

A GAMS parameter with domain and values.
"""
struct GDXParameter <: GDXSymbol
    name::String
    description::String
    domain::Vector{String}
    records::DataFrames.DataFrame
end

"""
    GDXVariable

A GAMS variable with level, marginal, lower, upper, and scale values.
"""
struct GDXVariable <: GDXSymbol
    name::String
    description::String
    domain::Vector{String}
    vartype::Int
    records::DataFrames.DataFrame
end

"""
    GDXEquation

A GAMS equation with level, marginal, lower, upper, and scale values.
"""
struct GDXEquation <: GDXSymbol
    name::String
    description::String
    domain::Vector{String}
    equtype::Int
    records::DataFrames.DataFrame
end

# =============================================================================
# GDXFile container
# =============================================================================

"""
    GDXFile

Container for GDX file contents. Provides dictionary-like access to symbols.

# Example
```julia
gdx = read_gdx("model.gdx")
gdx[:demand]              # Access records as DataFrame
get_symbol(gdx, :demand)  # Access full GDXSymbol object
list_parameters(gdx)      # List all parameters
```
"""
struct GDXFile
    path::String
    symbols::Dict{Symbol, GDXSymbol}
end

function Base.show(io::IO, gdx::GDXFile)
    println(io, "GDXFile: ", gdx.path)
    sets = list_sets(gdx)
    params = list_parameters(gdx)
    vars = list_variables(gdx)
    eqns = list_equations(gdx)
    isempty(sets) || println(io, "  Sets ($(length(sets))): ", join(sets, ", "))
    isempty(params) || println(io, "  Parameters ($(length(params))): ", join(params, ", "))
    isempty(vars) || println(io, "  Variables ($(length(vars))): ", join(vars, ", "))
    isempty(eqns) || println(io, "  Equations ($(length(eqns))): ", join(eqns, ", "))
end

# Symbol listing functions
list_sets(gdx::GDXFile) = Symbol[k for (k, v) in gdx.symbols if v isa GDXSet]
list_parameters(gdx::GDXFile) = Symbol[k for (k, v) in gdx.symbols if v isa GDXParameter]
list_variables(gdx::GDXFile) = Symbol[k for (k, v) in gdx.symbols if v isa GDXVariable]
list_equations(gdx::GDXFile) = Symbol[k for (k, v) in gdx.symbols if v isa GDXEquation]
list_symbols(gdx::GDXFile) = collect(keys(gdx.symbols))

"""
    get_symbol(gdx::GDXFile, sym) -> GDXSymbol

Return the full GDXSymbol object (with name, description, domain, etc.),
not just the records DataFrame.
"""
get_symbol(gdx::GDXFile, sym::Symbol) = gdx.symbols[sym]
get_symbol(gdx::GDXFile, sym::String) = gdx.symbols[Symbol(sym)]

# Dictionary-like access (returns records DataFrame)
Base.getindex(gdx::GDXFile, sym::Symbol) = gdx.symbols[sym].records
Base.getindex(gdx::GDXFile, sym::String) = gdx[Symbol(sym)]
Base.haskey(gdx::GDXFile, sym::Symbol) = haskey(gdx.symbols, sym)
Base.keys(gdx::GDXFile) = keys(gdx.symbols)
Base.length(gdx::GDXFile) = length(gdx.symbols)
Base.iterate(gdx::GDXFile) = iterate(gdx.symbols)
Base.iterate(gdx::GDXFile, state) = iterate(gdx.symbols, state)

# Property access for tab completion
function Base.propertynames(gdx::GDXFile, private::Bool=false)
    (fieldnames(GDXFile)..., keys(gdx.symbols)...)
end

function Base.getproperty(gdx::GDXFile, sym::Symbol)
    sym in fieldnames(GDXFile) && return getfield(gdx, sym)
    haskey(gdx.symbols, sym) && return gdx.symbols[sym].records
    error("Symbol :$sym not found in GDX file")
end

# =============================================================================
# Reading GDX files
# =============================================================================

"""
    read_gdx(filepath::String; parse_integers=true, only=nothing) -> GDXFile

Read a GDX file and return a GDXFile container with all symbols.

# Arguments
- `filepath`: Path to the GDX file
- `parse_integers`: If true, attempt to parse set elements that look like integers as Int
- `only`: Optional collection of symbol names (Strings or Symbols) to read.
  When provided, only the specified symbols are loaded from the file.

# Example
```julia
gdx = read_gdx("transport.gdx")
demand = gdx[:demand]  # Get parameter as DataFrame

# Read only specific symbols from a large file
gdx = read_gdx("big_model.gdx", only=[:x, :demand])
```
"""
function read_gdx(filepath::String; parse_integers::Bool=true, only=nothing)
    gdx = GDXHandle()
    gdx_create(gdx)
    only_filter = only === nothing ? nothing : Set{Symbol}(Symbol.(only))

    try
        gdx_open_read(gdx, filepath)
        symbols = Dict{Symbol, GDXSymbol}()

        n_syms, n_uels = gdx_system_info(gdx)

        for sym_nr in 1:n_syms
            sym_name, sym_dim, sym_type = gdx_symbol_info(gdx, sym_nr)
            sym_key = Symbol(sym_name)

            if only_filter !== nothing && !(sym_key in only_filter)
                continue
            end

            sym_count, sym_user_info, sym_description = gdx_symbol_info_x(gdx, sym_nr)

            if sym_type == GMS_DT_SET
                symbols[sym_key] = _read_set(gdx, sym_nr, sym_name, sym_dim, sym_description)
            elseif sym_type == GMS_DT_PAR
                symbols[sym_key] = _read_parameter(gdx, sym_nr, sym_name, sym_dim, sym_description, parse_integers)
            elseif sym_type == GMS_DT_VAR
                symbols[sym_key] = _read_variable(gdx, sym_nr, sym_name, sym_dim, sym_description, sym_user_info, parse_integers)
            elseif sym_type == GMS_DT_EQU
                symbols[sym_key] = _read_equation(gdx, sym_nr, sym_name, sym_dim, sym_description, sym_user_info, parse_integers)
            end
            # Skip aliases (GMS_DT_ALIAS)
        end

        gdx_close(gdx)
        return GDXFile(filepath, symbols)
    finally
        gdx_free(gdx)
    end
end

function _read_set(gdx::GDXHandle, sym_nr::Int, name::String, dim::Int, description::String)
    domains = dim > 0 ? gdx_symbol_get_domain_x(gdx, sym_nr, dim) : String[]

    n_recs = gdx_data_read_str_start(gdx, sym_nr)

    keys = Vector{String}(undef, max(dim, 1))
    vals = Vector{Float64}(undef, GMS_VAL_MAX)
    columns = [Vector{String}(undef, n_recs) for _ in 1:dim]

    for i in 1:n_recs
        gdx_data_read_str(gdx, keys, vals)
        for d in 1:dim
            columns[d][i] = keys[d]
        end
    end
    gdx_data_read_done(gdx)

    df = DataFrames.DataFrame()
    for (d, domain) in enumerate(domains)
        col_name = domain == "*" ? "dim$d" : domain
        df[!, col_name] = columns[d]
    end

    return GDXSet(name, description, domains, df)
end

function _read_parameter(gdx::GDXHandle, sym_nr::Int, name::String, dim::Int, description::String, parse_integers::Bool)
    domains = dim > 0 ? gdx_symbol_get_domain_x(gdx, sym_nr, dim) : String[]

    n_recs = gdx_data_read_str_start(gdx, sym_nr)

    keys = Vector{String}(undef, max(dim, 1))
    vals = Vector{Float64}(undef, GMS_VAL_MAX)
    columns = [Vector{String}(undef, n_recs) for _ in 1:dim]
    values = Vector{Float64}(undef, n_recs)

    for i in 1:n_recs
        gdx_data_read_str(gdx, keys, vals)
        for d in 1:dim
            columns[d][i] = keys[d]
        end
        values[i] = parse_gdx_value(vals[GAMS_VALUE_LEVEL])
    end
    gdx_data_read_done(gdx)

    df = DataFrames.DataFrame()
    for (d, domain) in enumerate(domains)
        col_name = domain == "*" ? "dim$d" : domain
        col_data = columns[d]
        if parse_integers
            col_data = _try_parse_integers(col_data)
        end
        df[!, col_name] = col_data
    end
    df[!, :value] = values

    DataFrames.metadata!(df, "name", name, style=:default)
    DataFrames.metadata!(df, "description", description, style=:default)

    return GDXParameter(name, description, domains, df)
end

function _read_variable(gdx::GDXHandle, sym_nr::Int, name::String, dim::Int, description::String, user_info::Int, parse_integers::Bool)
    domains = dim > 0 ? gdx_symbol_get_domain_x(gdx, sym_nr, dim) : String[]

    n_recs = gdx_data_read_str_start(gdx, sym_nr)

    keys = Vector{String}(undef, max(dim, 1))
    vals = Vector{Float64}(undef, GMS_VAL_MAX)
    columns = [Vector{String}(undef, n_recs) for _ in 1:dim]
    level = Vector{Float64}(undef, n_recs)
    marginal = Vector{Float64}(undef, n_recs)
    lower = Vector{Float64}(undef, n_recs)
    upper = Vector{Float64}(undef, n_recs)
    scale = Vector{Float64}(undef, n_recs)

    for i in 1:n_recs
        gdx_data_read_str(gdx, keys, vals)
        for d in 1:dim
            columns[d][i] = keys[d]
        end
        level[i] = parse_gdx_value(vals[GAMS_VALUE_LEVEL])
        marginal[i] = parse_gdx_value(vals[GAMS_VALUE_MARGINAL])
        lower[i] = parse_gdx_value(vals[GAMS_VALUE_LOWER])
        upper[i] = parse_gdx_value(vals[GAMS_VALUE_UPPER])
        scale[i] = parse_gdx_value(vals[GAMS_VALUE_SCALE])
    end
    gdx_data_read_done(gdx)

    df = DataFrames.DataFrame()
    for (d, domain) in enumerate(domains)
        col_name = domain == "*" ? "dim$d" : domain
        col_data = columns[d]
        if parse_integers
            col_data = _try_parse_integers(col_data)
        end
        df[!, col_name] = col_data
    end
    df[!, :level] = level
    df[!, :marginal] = marginal
    df[!, :lower] = lower
    df[!, :upper] = upper
    df[!, :scale] = scale

    DataFrames.metadata!(df, "name", name, style=:default)
    DataFrames.metadata!(df, "description", description, style=:default)

    return GDXVariable(name, description, domains, user_info, df)
end

function _read_equation(gdx::GDXHandle, sym_nr::Int, name::String, dim::Int, description::String, user_info::Int, parse_integers::Bool)
    domains = dim > 0 ? gdx_symbol_get_domain_x(gdx, sym_nr, dim) : String[]

    n_recs = gdx_data_read_str_start(gdx, sym_nr)

    keys = Vector{String}(undef, max(dim, 1))
    vals = Vector{Float64}(undef, GMS_VAL_MAX)
    columns = [Vector{String}(undef, n_recs) for _ in 1:dim]
    level = Vector{Float64}(undef, n_recs)
    marginal = Vector{Float64}(undef, n_recs)
    lower = Vector{Float64}(undef, n_recs)
    upper = Vector{Float64}(undef, n_recs)
    scale = Vector{Float64}(undef, n_recs)

    for i in 1:n_recs
        gdx_data_read_str(gdx, keys, vals)
        for d in 1:dim
            columns[d][i] = keys[d]
        end
        level[i] = parse_gdx_value(vals[GAMS_VALUE_LEVEL])
        marginal[i] = parse_gdx_value(vals[GAMS_VALUE_MARGINAL])
        lower[i] = parse_gdx_value(vals[GAMS_VALUE_LOWER])
        upper[i] = parse_gdx_value(vals[GAMS_VALUE_UPPER])
        scale[i] = parse_gdx_value(vals[GAMS_VALUE_SCALE])
    end
    gdx_data_read_done(gdx)

    df = DataFrames.DataFrame()
    for (d, domain) in enumerate(domains)
        col_name = domain == "*" ? "dim$d" : domain
        col_data = columns[d]
        if parse_integers
            col_data = _try_parse_integers(col_data)
        end
        df[!, col_name] = col_data
    end
    df[!, :level] = level
    df[!, :marginal] = marginal
    df[!, :lower] = lower
    df[!, :upper] = upper
    df[!, :scale] = scale

    DataFrames.metadata!(df, "name", name, style=:default)
    DataFrames.metadata!(df, "description", description, style=:default)

    return GDXEquation(name, description, domains, user_info, df)
end

# =============================================================================
# Writing GDX files
# =============================================================================

"""
    write_gdx(filepath::String, gdxfile::GDXFile; producer="GDXInterface.jl")

Write a GDXFile container (with sets, parameters, variables, and equations) to a GDX file.

# Example
```julia
gdx = read_gdx("input.gdx")
write_gdx("output.gdx", gdx)
```
"""
function write_gdx(filepath::String, gdxfile::GDXFile; producer::String="GDXInterface.jl")
    gdx = GDXHandle()
    gdx_create(gdx)

    try
        gdx_open_write(gdx, filepath, producer)

        for (_, sym) in gdxfile.symbols
            _write_symbol(gdx, sym)
        end

        gdx_close(gdx)
    finally
        gdx_free(gdx)
    end
    return filepath
end

"""
    write_gdx(filepath::String, symbols::Pair{String, DataFrame}...; producer="GDXInterface.jl")

Write DataFrames to a GDX file as parameters. Each pair maps a symbol name to its DataFrame.
The DataFrame must have a `:value` column; all other columns are treated as domain dimensions.

# Example
```julia
df = DataFrame(i=["a", "b", "c"], value=[1.0, 2.0, 3.0])
write_gdx("output.gdx", "demand" => df)
```
"""
function write_gdx(filepath::String, symbols::Pair{String, DataFrames.DataFrame}...; producer::String="GDXInterface.jl")
    gdx = GDXHandle()
    gdx_create(gdx)

    try
        gdx_open_write(gdx, filepath, producer)

        for (name, df) in symbols
            desc = get(DataFrames.metadata(df), "description", "")
            _write_parameter_df(gdx, name, df, desc)
        end

        gdx_close(gdx)
    finally
        gdx_free(gdx)
    end
    return filepath
end

# Type dispatch for writing symbols
_write_symbol(gdx::GDXHandle, sym::GDXSet) = _write_set(gdx, sym)
_write_symbol(gdx::GDXHandle, sym::GDXParameter) = _write_parameter(gdx, sym)
_write_symbol(gdx::GDXHandle, sym::GDXVariable) = _write_variable(gdx, sym)
_write_symbol(gdx::GDXHandle, sym::GDXEquation) = _write_equation(gdx, sym)

function _write_set(gdx::GDXHandle, sym::GDXSet)
    df = sym.records
    cols = names(df)
    dim = length(cols)

    gdx_data_write_str_start(gdx, sym.name, sym.description, dim, GMS_DT_SET)

    keys = Vector{String}(undef, dim)
    vals = zeros(Float64, GMS_VAL_MAX)

    for row in eachrow(df)
        for (i, col) in enumerate(cols)
            keys[i] = string(row[col])
        end
        gdx_data_write_str(gdx, keys, vals)
    end

    gdx_data_write_done(gdx)
end

function _write_parameter(gdx::GDXHandle, sym::GDXParameter)
    _write_parameter_df(gdx, sym.name, sym.records, sym.description)
end

function _write_parameter_df(gdx::GDXHandle, name::String, df::DataFrames.DataFrame, description::String="")
    dim_cols = [n for n in names(df) if n != "value"]
    dim = length(dim_cols)

    gdx_data_write_str_start(gdx, name, description, dim, GMS_DT_PAR)

    keys = Vector{String}(undef, dim)
    vals = zeros(Float64, GMS_VAL_MAX)

    for row in eachrow(df)
        for (i, col) in enumerate(dim_cols)
            keys[i] = string(row[col])
        end
        vals[GAMS_VALUE_LEVEL] = _to_gdx_value(row[:value])
        gdx_data_write_str(gdx, keys, vals)
    end

    gdx_data_write_done(gdx)
end

const _VAR_EQU_COLS = Set(["level", "marginal", "lower", "upper", "scale"])

function _write_variable(gdx::GDXHandle, sym::GDXVariable)
    df = sym.records
    dim_cols = [n for n in names(df) if !(n in _VAR_EQU_COLS)]
    dim = length(dim_cols)

    gdx_data_write_str_start(gdx, sym.name, sym.description, dim, GMS_DT_VAR, sym.vartype)

    keys = Vector{String}(undef, dim)
    vals = zeros(Float64, GMS_VAL_MAX)

    for row in eachrow(df)
        for (i, col) in enumerate(dim_cols)
            keys[i] = string(row[col])
        end
        vals[GAMS_VALUE_LEVEL] = _to_gdx_value(row[:level])
        vals[GAMS_VALUE_MARGINAL] = _to_gdx_value(row[:marginal])
        vals[GAMS_VALUE_LOWER] = _to_gdx_value(row[:lower])
        vals[GAMS_VALUE_UPPER] = _to_gdx_value(row[:upper])
        vals[GAMS_VALUE_SCALE] = _to_gdx_value(row[:scale])
        gdx_data_write_str(gdx, keys, vals)
    end

    gdx_data_write_done(gdx)
end

function _write_equation(gdx::GDXHandle, sym::GDXEquation)
    df = sym.records
    dim_cols = [n for n in names(df) if !(n in _VAR_EQU_COLS)]
    dim = length(dim_cols)

    gdx_data_write_str_start(gdx, sym.name, sym.description, dim, GMS_DT_EQU, sym.equtype)

    keys = Vector{String}(undef, dim)
    vals = zeros(Float64, GMS_VAL_MAX)

    for row in eachrow(df)
        for (i, col) in enumerate(dim_cols)
            keys[i] = string(row[col])
        end
        vals[GAMS_VALUE_LEVEL] = _to_gdx_value(row[:level])
        vals[GAMS_VALUE_MARGINAL] = _to_gdx_value(row[:marginal])
        vals[GAMS_VALUE_LOWER] = _to_gdx_value(row[:lower])
        vals[GAMS_VALUE_UPPER] = _to_gdx_value(row[:upper])
        vals[GAMS_VALUE_SCALE] = _to_gdx_value(row[:scale])
        gdx_data_write_str(gdx, keys, vals)
    end

    gdx_data_write_done(gdx)
end

# =============================================================================
# Utilities
# =============================================================================

function _try_parse_integers(strings::Vector{String})
    all_ints = all(s -> !isnothing(tryparse(Int, s)), strings)
    all_ints && return parse.(Int, strings)
    return strings
end

function _to_gdx_value(val::Float64)
    isnan(val) && return GAMS_SV_NA
    val == Inf && return GAMS_SV_PINF
    val == -Inf && return GAMS_SV_MINF
    return val
end

_to_gdx_value(val::Real) = _to_gdx_value(Float64(val))
