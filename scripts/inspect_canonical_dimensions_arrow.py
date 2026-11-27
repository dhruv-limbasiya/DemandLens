import pyarrow.dataset as ds
import pyarrow.compute as pc
import json
import os

def main():
    # Path to canonical jobs parquet directory
    data_path = os.path.abspath(os.path.join('data', 'silver', 'canonical', 'jobs'))
    # Load dataset (can handle partitioned directories)
    dataset = ds.dataset(data_path, format='parquet')

    dimensions = ["source_id", "country_code", "workplace_type", "seniority_tier", "freshness_category"]
    results = {}
    for dim in dimensions:
        # Scan only the column
        table = dataset.to_table(columns=[dim])
        col = table[dim]
        # Compute distinct values
        uniq = pc.unique(col)
        # Convert to python list (handle nulls separately)
        # uniq may contain null; we'll count nulls explicitly
        null_count = pc.sum(pc.is_null(col)).as_py()
        # Remove null from distinct values list if present
        uniq_vals = [v.as_py() for v in uniq if v is not None]
        results[dim] = {
            "distinct_count": len(uniq_vals),
            "null_count": null_count,
            "distinct_values": uniq_vals,
        }
    # Cube cardinality = product of distinct counts (excluding nulls)
    cardinality = 1
    for info in results.values():
        cardinality *= max(info["distinct_count"], 1)
    results["cube_cardinality"] = cardinality
    # Include schema fields for reference
    schema = {name: str(dataset.schema.field_by_name(name).type) for name in dimensions if dataset.schema.get_field_index(name) != -1}
    results["schema"] = schema
    print(json.dumps(results, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
