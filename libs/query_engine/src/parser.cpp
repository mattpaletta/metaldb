#include <metaldb/query_engine/parser.hpp>
#include <metaldb/query_engine/AST/filter.hpp>
#include <metaldb/query_engine/AST/join.hpp>
#include <metaldb/query_engine/AST/limit.hpp>
#include <metaldb/query_engine/AST/read.hpp>
#include <metaldb/query_engine/AST/rho.hpp>
#include <metaldb/query_engine/AST/projection.hpp>
#include <metaldb/query_engine/AST/write.hpp>

auto metaldb::QueryEngine::Parser::Parse(const std::string& query) const -> std::shared_ptr<AST::Expr> {
    std::shared_ptr<AST::Expr> expr;

    /**
     * SELECT * FROM mytable;
     */
    expr = std::make_shared<AST::Read>("taxi");

    /**
     * SELECT colA, colB FROM mytable;
     */
    expr = std::make_shared<AST::Projection>(std::vector<std::string>{"lpep_pickup_datetime", "lpep_dropoff_datetime"},
                                             std::make_shared<AST::Read>("taxi"));


    expr = std::make_shared<AST::Projection>(std::vector<std::string>{"sepal.length", "petal.length"},
                                             std::make_shared<AST::Read>("iris"));
    return std::make_shared<AST::Write>("output", std::vector<std::string>{"sepal_length", "petal_length"}, CSV, expr);


    /**
     * SELECT colA, colB as colC FROM mytable;
     */
    expr = std::make_shared<AST::Rho>("colB", "colC",
                                      std::make_shared<AST::Projection>(std::vector<std::string>{"colA", "colB"},
                                                                        std::make_shared<AST::Read>("mytable")));

    /**
     * SELECT * FROM mytable WHERE colA > 5.0;
     */
    expr = std::make_shared<AST::Filter>(std::make_shared<AST::GTOperator>(std::make_shared<AST::ReadColumn>("colA"),
                                                                           std::make_shared<AST::ConstantFloat>(5.0)),
                                         std::make_shared<AST::Read>("mytable"));

    /**
     * SELECT * FROM mytable WHERE colA > 5.0 AND colB < 2.0;
     */
    expr = std::make_shared<AST::Filter>(std::make_shared<AST::AndOperator>(std::make_shared<AST::GTOperator>(std::make_shared<AST::ReadColumn>("colA"),
                                                                                                              std::make_shared<AST::ConstantFloat>(5.0)),
                                                                            std::make_shared<AST::LTOperator>(std::make_shared<AST::ReadColumn>("colB"),
                                                                                                              std::make_shared<AST::ConstantFloat>(2.0))),
                                         std::make_shared<AST::Read>("mytable"));

    /**
     * SELECT * FROM mytable JOIN mytable2 ON mytable.colA = mytable2.colA;
     */
    expr = std::make_shared<AST::Join>(AST::Join::JoinType::NATURAL,
                                       std::make_shared<AST::EqOperator>(std::make_shared<AST::ReadColumn>("mytable", "colA"),
                                                                         std::make_shared<AST::ReadColumn>("mytable2", "colB")),
                                       std::make_shared<AST::Read>("mytable2"),
                                       std::make_shared<AST::Read>("mytable"));

    /**
     * SELECT colA, colB FROM mytable LIMIT 10;
     */
    expr = std::make_shared<AST::Limit>(10,
                                        std::make_shared<AST::Projection>(std::vector<std::string>{"colA", "colB"},
                                                                          std::make_shared<AST::Read>("mytable")));

    return expr;
}
