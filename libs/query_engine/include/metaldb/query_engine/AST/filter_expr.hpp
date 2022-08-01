#pragma once

#include <memory>
#include <string>

namespace metaldb::QueryEngine::AST {
    class BaseFilterExpr {
    public:
        BaseFilterExpr() = default;
        virtual ~BaseFilterExpr() noexcept = default;
    };

    class ConstantInt : public BaseFilterExpr {
    public:
        ConstantInt(int value) : _value(value) {}
        ~ConstantInt() noexcept = default;

    private:
        int _value;
    };

    class ConstantFloat : public BaseFilterExpr {
    public:
        ConstantFloat(float value) : _value(value) {}
        ~ConstantFloat() noexcept = default;

    private:
        float _value;
    };

    class ConstantString : public BaseFilterExpr {
    public:
        ConstantString(std::string value) : _value(std::move(value)) {}
        ~ConstantString() noexcept = default;

    private:
        std::string _value;
    };

    class ReadColumn : public BaseFilterExpr {
    public:
        ReadColumn(std::string table, std::string column) : _table(std::move(table)), _column(std::move(column)) {}
        ReadColumn(std::string column) : _table(""), _column(std::move(column)) {}
        ~ReadColumn() noexcept = default;

    private:
        std::string _table;
        std::string _column;
    };

    class LTOperator : public BaseFilterExpr {
    public:
        LTOperator(std::shared_ptr<BaseFilterExpr> lhs, std::shared_ptr<BaseFilterExpr> rhs) : _lhs(std::move(lhs)), _rhs(std::move(rhs)) {}
        ~LTOperator() noexcept = default;

    private:
        std::shared_ptr<BaseFilterExpr> _lhs;
        std::shared_ptr<BaseFilterExpr> _rhs;
    };

    class GTOperator : public BaseFilterExpr {
    public:
        GTOperator(std::shared_ptr<BaseFilterExpr> lhs, std::shared_ptr<BaseFilterExpr> rhs) : _lhs(std::move(lhs)), _rhs(std::move(rhs)) {}
        ~GTOperator() noexcept = default;

    private:
        std::shared_ptr<BaseFilterExpr> _lhs;
        std::shared_ptr<BaseFilterExpr> _rhs;
    };

    class AndOperator : public BaseFilterExpr {
    public:
        AndOperator(std::shared_ptr<BaseFilterExpr> lhs, std::shared_ptr<BaseFilterExpr> rhs) : _lhs(std::move(lhs)), _rhs(std::move(rhs)) {}
        ~AndOperator() noexcept = default;

    private:
        std::shared_ptr<BaseFilterExpr> _lhs;
        std::shared_ptr<BaseFilterExpr> _rhs;
    };

    class OrOperator : public BaseFilterExpr {
    public:
        OrOperator(std::shared_ptr<BaseFilterExpr> lhs, std::shared_ptr<BaseFilterExpr> rhs) : _lhs(std::move(lhs)), _rhs(std::move(rhs)) {}
        ~OrOperator() noexcept = default;

    private:
        std::shared_ptr<BaseFilterExpr> _lhs;
        std::shared_ptr<BaseFilterExpr> _rhs;
    };

    class EqOperator : public BaseFilterExpr {
    public:
        EqOperator(std::shared_ptr<BaseFilterExpr> lhs, std::shared_ptr<BaseFilterExpr> rhs) : _lhs(std::move(lhs)), _rhs(std::move(rhs)) {}
        ~EqOperator() noexcept = default;

    private:
        std::shared_ptr<BaseFilterExpr> _lhs;
        std::shared_ptr<BaseFilterExpr> _rhs;
    };
}
