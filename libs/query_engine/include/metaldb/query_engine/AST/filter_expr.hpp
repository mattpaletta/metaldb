#pragma once

#include <memory>
#include <string>

namespace metaldb::QueryEngine::AST {
    class BaseFilterExpr {
    public:
        BaseFilterExpr() = default;
        virtual ~BaseFilterExpr() = default;
    };

    class ConstantInt : public BaseFilterExpr {
    public:
        ConstantInt(int value) : _value(value) {}
        ~ConstantInt() = default;

    private:
        int _value;
    };

    class ConstantFloat : public BaseFilterExpr {
    public:
        ConstantFloat(float value) : _value(value) {}
        ~ConstantFloat() = default;

    private:
        float _value;
    };

    class ConstantString : public BaseFilterExpr {
    public:
        ConstantString(const std::string& value) : _value(value) {}
        ~ConstantString() = default;

    private:
        std::string _value;
    };

    class ReadColumn : public BaseFilterExpr {
    public:
        ReadColumn(const std::string& table, const std::string& column) : _table(table), _column(column) {}
        ReadColumn(const std::string& column) : _table(""), _column(column) {}
        ~ReadColumn() = default;

    private:
        std::string _table;
        std::string _column;
    };

    class LTOperator : public BaseFilterExpr {
    public:
        LTOperator(std::shared_ptr<BaseFilterExpr> lhs, std::shared_ptr<BaseFilterExpr> rhs) : _lhs(lhs), _rhs(rhs) {}
        ~LTOperator() = default;

    private:
        std::shared_ptr<BaseFilterExpr> _lhs;
        std::shared_ptr<BaseFilterExpr> _rhs;
    };

    class GTOperator : public BaseFilterExpr {
    public:
        GTOperator(std::shared_ptr<BaseFilterExpr> lhs, std::shared_ptr<BaseFilterExpr> rhs) : _lhs(lhs), _rhs(rhs) {}
        ~GTOperator() = default;

    private:
        std::shared_ptr<BaseFilterExpr> _lhs;
        std::shared_ptr<BaseFilterExpr> _rhs;
    };

    class AndOperator : public BaseFilterExpr {
    public:
        AndOperator(std::shared_ptr<BaseFilterExpr> lhs, std::shared_ptr<BaseFilterExpr> rhs) : _lhs(lhs), _rhs(rhs) {}
        ~AndOperator() = default;

    private:
        std::shared_ptr<BaseFilterExpr> _lhs;
        std::shared_ptr<BaseFilterExpr> _rhs;
    };

    class OrOperator : public BaseFilterExpr {
    public:
        OrOperator(std::shared_ptr<BaseFilterExpr> lhs, std::shared_ptr<BaseFilterExpr> rhs) : _lhs(lhs), _rhs(rhs) {}
        ~OrOperator() = default;

    private:
        std::shared_ptr<BaseFilterExpr> _lhs;
        std::shared_ptr<BaseFilterExpr> _rhs;
    };

    class EqOperator : public BaseFilterExpr {
    public:
        EqOperator(std::shared_ptr<BaseFilterExpr> lhs, std::shared_ptr<BaseFilterExpr> rhs) : _lhs(lhs), _rhs(rhs) {}
        ~EqOperator() = default;

    private:
        std::shared_ptr<BaseFilterExpr> _lhs;
        std::shared_ptr<BaseFilterExpr> _rhs;
    };
}
