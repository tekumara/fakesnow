#include "duckdb_extension.h"

#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef enum {
	FS_SUM_INT8,
	FS_SUM_INT16,
	FS_SUM_INT32,
	FS_SUM_INT64,
	FS_SUM_UINT8,
	FS_SUM_UINT16,
	FS_SUM_UINT32,
	FS_SUM_UINT64,
	FS_SUM_FLOAT,
	FS_SUM_DOUBLE,
	FS_SUM_VARCHAR
} FsSumType;

typedef struct {
	FsSumType type;
} FsSumInfo;

typedef struct {
	int64_t sum;
	uint64_t count;
} FsIntSumState;

typedef struct {
	double sum;
	uint64_t count;
} FsDoubleSumState;

static idx_t FsIntSumSize(duckdb_function_info info) {
	return sizeof(FsIntSumState);
}

static idx_t FsDoubleSumSize(duckdb_function_info info) {
	return sizeof(FsDoubleSumState);
}

static void FsIntSumInit(duckdb_function_info info, duckdb_aggregate_state state_p) {
	FsIntSumState *state = (FsIntSumState *)state_p;
	state->sum = 0;
	state->count = 0;
}

static void FsDoubleSumInit(duckdb_function_info info, duckdb_aggregate_state state_p) {
	FsDoubleSumState *state = (FsDoubleSumState *)state_p;
	state->sum = 0;
	state->count = 0;
}

static bool FsStringToDouble(duckdb_string_t *str, double *out) {
	uint32_t length = duckdb_string_t_length(*str);
	const char *data = duckdb_string_t_data(str);
	char *copy = (char *)malloc((size_t)length + 1);
	if (!copy) {
		return false;
	}
	memcpy(copy, data, length);
	copy[length] = '\0';

	errno = 0;
	char *end = NULL;
	double value = strtod(copy, &end);
	while (end && isspace((unsigned char)*end)) {
		end++;
	}
	bool ok = end && *end == '\0' && end != copy && errno != ERANGE;
	free(copy);
	if (!ok) {
		return false;
	}
	*out = value;
	return true;
}

static void FsIntSumUpdate(duckdb_function_info info, duckdb_data_chunk input, duckdb_aggregate_state *states_p) {
	FsSumInfo *sum_info = (FsSumInfo *)duckdb_aggregate_function_get_extra_info(info);
	FsIntSumState **states = (FsIntSumState **)states_p;
	idx_t row_count = duckdb_data_chunk_get_size(input);
	duckdb_vector input_vector = duckdb_data_chunk_get_vector(input, 0);
	uint64_t *validity = duckdb_vector_get_validity(input_vector);
	void *data = duckdb_vector_get_data(input_vector);

	for (idx_t row = 0; row < row_count; row++) {
		if (!duckdb_validity_row_is_valid(validity, row)) {
			continue;
		}
		int64_t value = 0;
		switch (sum_info->type) {
		case FS_SUM_INT8:
			value = ((int8_t *)data)[row];
			break;
		case FS_SUM_INT16:
			value = ((int16_t *)data)[row];
			break;
		case FS_SUM_INT32:
			value = ((int32_t *)data)[row];
			break;
		case FS_SUM_INT64:
			value = ((int64_t *)data)[row];
			break;
		case FS_SUM_UINT8:
			value = ((uint8_t *)data)[row];
			break;
		case FS_SUM_UINT16:
			value = ((uint16_t *)data)[row];
			break;
		case FS_SUM_UINT32:
			value = ((uint32_t *)data)[row];
			break;
		case FS_SUM_UINT64:
			value = (int64_t)((uint64_t *)data)[row];
			break;
		default:
			duckdb_aggregate_function_set_error(info, "Invalid integer sum input type");
			return;
		}
		states[row]->sum += value;
		states[row]->count++;
	}
}

static void FsDoubleSumUpdate(duckdb_function_info info, duckdb_data_chunk input, duckdb_aggregate_state *states_p) {
	FsSumInfo *sum_info = (FsSumInfo *)duckdb_aggregate_function_get_extra_info(info);
	FsDoubleSumState **states = (FsDoubleSumState **)states_p;
	idx_t row_count = duckdb_data_chunk_get_size(input);
	duckdb_vector input_vector = duckdb_data_chunk_get_vector(input, 0);
	uint64_t *validity = duckdb_vector_get_validity(input_vector);
	void *data = duckdb_vector_get_data(input_vector);

	for (idx_t row = 0; row < row_count; row++) {
		if (!duckdb_validity_row_is_valid(validity, row)) {
			continue;
		}
		double value = 0;
		switch (sum_info->type) {
		case FS_SUM_FLOAT:
			value = ((float *)data)[row];
			break;
		case FS_SUM_DOUBLE:
			value = ((double *)data)[row];
			break;
		case FS_SUM_VARCHAR:
			if (!FsStringToDouble(&((duckdb_string_t *)data)[row], &value)) {
				continue;
			}
			break;
		default:
			duckdb_aggregate_function_set_error(info, "Invalid double sum input type");
			return;
		}
		states[row]->sum += value;
		states[row]->count++;
	}
}

static void FsIntSumCombine(duckdb_function_info info, duckdb_aggregate_state *source_p,
                            duckdb_aggregate_state *target_p, idx_t count) {
	FsIntSumState **source = (FsIntSumState **)source_p;
	FsIntSumState **target = (FsIntSumState **)target_p;
	for (idx_t i = 0; i < count; i++) {
		target[i]->sum += source[i]->sum;
		target[i]->count += source[i]->count;
	}
}

static void FsDoubleSumCombine(duckdb_function_info info, duckdb_aggregate_state *source_p,
                               duckdb_aggregate_state *target_p, idx_t count) {
	FsDoubleSumState **source = (FsDoubleSumState **)source_p;
	FsDoubleSumState **target = (FsDoubleSumState **)target_p;
	for (idx_t i = 0; i < count; i++) {
		target[i]->sum += source[i]->sum;
		target[i]->count += source[i]->count;
	}
}

static void FsIntSumFinalize(duckdb_function_info info, duckdb_aggregate_state *source_p, duckdb_vector result,
                             idx_t count, idx_t offset) {
	FsIntSumState **source = (FsIntSumState **)source_p;
	int64_t *result_data = (int64_t *)duckdb_vector_get_data(result);
	duckdb_vector_ensure_validity_writable(result);
	uint64_t *result_validity = duckdb_vector_get_validity(result);
	for (idx_t i = 0; i < count; i++) {
		if (source[i]->count == 0) {
			duckdb_validity_set_row_invalid(result_validity, offset + i);
		} else {
			result_data[offset + i] = source[i]->sum;
		}
	}
}

static void FsDoubleSumFinalize(duckdb_function_info info, duckdb_aggregate_state *source_p, duckdb_vector result,
                                idx_t count, idx_t offset) {
	FsDoubleSumState **source = (FsDoubleSumState **)source_p;
	double *result_data = (double *)duckdb_vector_get_data(result);
	duckdb_vector_ensure_validity_writable(result);
	uint64_t *result_validity = duckdb_vector_get_validity(result);
	for (idx_t i = 0; i < count; i++) {
		if (source[i]->count == 0) {
			duckdb_validity_set_row_invalid(result_validity, offset + i);
		} else {
			result_data[offset + i] = source[i]->sum;
		}
	}
}

static duckdb_aggregate_function FsCreateSumFunction(const char *name, duckdb_type input_type, duckdb_type return_type,
                                                     FsSumType sum_type, bool integer_sum) {
	duckdb_aggregate_function function = duckdb_create_aggregate_function();
	duckdb_aggregate_function_set_name(function, name);

	duckdb_logical_type parameter_type = duckdb_create_logical_type(input_type);
	duckdb_logical_type result_type = duckdb_create_logical_type(return_type);
	duckdb_aggregate_function_add_parameter(function, parameter_type);
	duckdb_aggregate_function_set_return_type(function, result_type);
	duckdb_destroy_logical_type(&parameter_type);
	duckdb_destroy_logical_type(&result_type);

	FsSumInfo *sum_info = (FsSumInfo *)malloc(sizeof(FsSumInfo));
	sum_info->type = sum_type;
	duckdb_aggregate_function_set_extra_info(function, sum_info, free);

	if (integer_sum) {
		duckdb_aggregate_function_set_functions(function, FsIntSumSize, FsIntSumInit, FsIntSumUpdate, FsIntSumCombine,
		                                        FsIntSumFinalize);
	} else {
		duckdb_aggregate_function_set_functions(function, FsDoubleSumSize, FsDoubleSumInit, FsDoubleSumUpdate,
		                                        FsDoubleSumCombine, FsDoubleSumFinalize);
	}
	return function;
}

static void FsAddSumOverload(duckdb_aggregate_function_set set, const char *name, duckdb_type input_type,
                             duckdb_type return_type, FsSumType sum_type, bool integer_sum) {
	duckdb_aggregate_function function = FsCreateSumFunction(name, input_type, return_type, sum_type, integer_sum);
	duckdb_add_aggregate_function_to_set(set, function);
	duckdb_destroy_aggregate_function(&function);
}

static void RegisterFsSumFunction(duckdb_connection connection) {
	const char *name = "_fs_sum";
	duckdb_aggregate_function_set set = duckdb_create_aggregate_function_set(name);

	FsAddSumOverload(set, name, DUCKDB_TYPE_TINYINT, DUCKDB_TYPE_BIGINT, FS_SUM_INT8, true);
	FsAddSumOverload(set, name, DUCKDB_TYPE_SMALLINT, DUCKDB_TYPE_BIGINT, FS_SUM_INT16, true);
	FsAddSumOverload(set, name, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_BIGINT, FS_SUM_INT32, true);
	FsAddSumOverload(set, name, DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_BIGINT, FS_SUM_INT64, true);
	FsAddSumOverload(set, name, DUCKDB_TYPE_UTINYINT, DUCKDB_TYPE_BIGINT, FS_SUM_UINT8, true);
	FsAddSumOverload(set, name, DUCKDB_TYPE_USMALLINT, DUCKDB_TYPE_BIGINT, FS_SUM_UINT16, true);
	FsAddSumOverload(set, name, DUCKDB_TYPE_UINTEGER, DUCKDB_TYPE_BIGINT, FS_SUM_UINT32, true);
	FsAddSumOverload(set, name, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BIGINT, FS_SUM_UINT64, true);
	FsAddSumOverload(set, name, DUCKDB_TYPE_FLOAT, DUCKDB_TYPE_DOUBLE, FS_SUM_FLOAT, false);
	FsAddSumOverload(set, name, DUCKDB_TYPE_DOUBLE, DUCKDB_TYPE_DOUBLE, FS_SUM_DOUBLE, false);
	FsAddSumOverload(set, name, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_DOUBLE, FS_SUM_VARCHAR, false);

	duckdb_register_aggregate_function_set(connection, set);
	duckdb_destroy_aggregate_function_set(&set);
}

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, struct duckdb_extension_access *access) {
	RegisterFsSumFunction(connection);
	return true;
}
