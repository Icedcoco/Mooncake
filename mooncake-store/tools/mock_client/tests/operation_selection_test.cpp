#include "mock_client/operation_selection.h"

#include <gtest/gtest.h>

namespace mock_client {
namespace {

Json::Value Operation(const char* name, const char* form, Json::UInt weight) {
    Json::Value value(Json::objectValue);
    value["name"] = name;
    value["form"] = form;
    value["weight"] = weight;
    return value;
}

Json::Value BatchOperation(const char* name, Json::UInt weight,
                           Json::UInt min, Json::UInt max) {
    Json::Value value = Operation(name, "batch", weight);
    value["batch_size"]["min"] = min;
    value["batch_size"]["max"] = max;
    return value;
}

}  // namespace

TEST(OperationSelectionTest, ParsesPutSingleAndRemoveSingle) {
    Json::Value input(Json::arrayValue);
    input.append(Operation("put", "single", 2));
    input.append(Operation("remove", "single", 1));

    auto result = ParseOperationSelection(input);

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 2);
    EXPECT_EQ(result->at(0).operation, LogicalOperation::Put);
    EXPECT_EQ(result->at(0).form, OperationForm::Single);
    EXPECT_EQ(result->at(0).weight, 2);
    EXPECT_EQ(result->at(1).operation, LogicalOperation::Remove);
    EXPECT_EQ(result->at(1).form, OperationForm::Single);
}

TEST(OperationSelectionTest, ParsesPutBatchSizeRange) {
    Json::Value input(Json::arrayValue);
    input.append(BatchOperation("put", 3, 4, 16));

    auto result = ParseOperationSelection(input);

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ(result->at(0).form, OperationForm::Batch);
    EXPECT_EQ(result->at(0).batch_size.min, 4);
    EXPECT_EQ(result->at(0).batch_size.max, 16);
}

TEST(OperationSelectionTest, PreservesMixedSelectionOrderAndStableNames) {
    Json::Value input(Json::arrayValue);
    input.append(Operation("remove", "single", 1));
    input.append(BatchOperation("put", 3, 2, 5));
    input.append(Operation("get", "single", 2));

    auto result = ParseOperationSelection(input);

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 3);
    EXPECT_EQ(ToString(result->at(0).operation), "remove");
    EXPECT_EQ(ToString(result->at(1).operation), "put");
    EXPECT_EQ(ToString(result->at(1).form), "batch");
    EXPECT_EQ(result->at(1).weight, 3);
    EXPECT_EQ(ToString(result->at(2).operation), "get");
}

TEST(OperationSelectionTest, ParsesMixedSingleAndBatchOverride) {
    auto result = ParseOperationOverride(
        "put:single=3,put:batch=2,remove:single=1");

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 3);
    EXPECT_EQ(result->at(0).operation, LogicalOperation::Put);
    EXPECT_EQ(result->at(0).form, OperationForm::Single);
    EXPECT_EQ(result->at(0).weight, 3);
    EXPECT_EQ(result->at(1).form, OperationForm::Batch);
    EXPECT_EQ(result->at(1).weight, 2);
    EXPECT_EQ(result->at(2).operation, LogicalOperation::Remove);
}

TEST(OperationSelectionTest, RejectsEmptySelection) {
    EXPECT_FALSE(ParseOperationSelection(Json::Value(Json::arrayValue))
                     .has_value());
    EXPECT_FALSE(ParseOperationOverride("").has_value());
}

TEST(OperationSelectionTest, RejectsUnknownOperationAndForm) {
    Json::Value unknown_operation(Json::arrayValue);
    unknown_operation.append(Operation("write", "single", 1));
    EXPECT_FALSE(ParseOperationSelection(unknown_operation).has_value());

    Json::Value unknown_form(Json::arrayValue);
    unknown_form.append(Operation("put", "stream", 1));
    EXPECT_FALSE(ParseOperationSelection(unknown_form).has_value());
}

TEST(OperationSelectionTest, RejectsZeroWeight) {
    Json::Value input(Json::arrayValue);
    input.append(Operation("put", "single", 0));

    EXPECT_FALSE(ParseOperationSelection(input).has_value());
    EXPECT_FALSE(ParseOperationOverride("put:single=0").has_value());
}

TEST(OperationSelectionTest, RejectsInvalidBatchRange) {
    Json::Value input(Json::arrayValue);
    input.append(BatchOperation("put", 1, 9, 2));

    EXPECT_FALSE(ParseOperationSelection(input).has_value());
}

TEST(OperationSelectionTest, RequiresBatchSizeForBatchEntry) {
    Json::Value input(Json::arrayValue);
    input.append(Operation("put", "batch", 1));

    EXPECT_FALSE(ParseOperationSelection(input).has_value());
}

TEST(OperationSelectionTest, RejectsBatchSizeOnSingleEntry) {
    Json::Value input(Json::arrayValue);
    Json::Value value = Operation("put", "single", 1);
    value["batch_size"]["min"] = 1;
    value["batch_size"]["max"] = 4;
    input.append(value);

    EXPECT_FALSE(ParseOperationSelection(input).has_value());
}

}  // namespace mock_client
