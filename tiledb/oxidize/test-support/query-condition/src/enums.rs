use tiledb_common::query::condition::{
    CombinationOp as RsCombinationOp, EqualityOp as RsEqualityOp, NullnessOp as RsNullnessOp,
    SetMembershipOp as RsSetMembershipOp,
};
use tiledb_oxidize::sm::enums::{QueryConditionCombinationOp, QueryConditionOp};

pub fn convert_equality_op(op: RsEqualityOp) -> QueryConditionOp {
    match op {
        RsEqualityOp::Less => QueryConditionOp::LT,
        RsEqualityOp::LessEqual => QueryConditionOp::LE,
        RsEqualityOp::Equal => QueryConditionOp::EQ,
        RsEqualityOp::NotEqual => QueryConditionOp::NE,
        RsEqualityOp::GreaterEqual => QueryConditionOp::GE,
        RsEqualityOp::Greater => QueryConditionOp::GT,
    }
}

pub fn convert_set_membership_op(op: RsSetMembershipOp) -> QueryConditionOp {
    match op {
        RsSetMembershipOp::In => QueryConditionOp::IN,
        RsSetMembershipOp::NotIn => QueryConditionOp::NOT_IN,
    }
}

pub fn convert_combination_op(op: RsCombinationOp) -> QueryConditionCombinationOp {
    match op {
        RsCombinationOp::And => QueryConditionCombinationOp::AND,
        RsCombinationOp::Or => QueryConditionCombinationOp::OR,
    }
}

pub fn convert_nullness_op(op: RsNullnessOp) -> QueryConditionOp {
    match op {
        RsNullnessOp::IsNull => QueryConditionOp::EQ,
        RsNullnessOp::NotNull => QueryConditionOp::NE,
    }
}
