package com.left;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by m2015 on 2017/6/5.
 *
 */
public class Expression {
    List<Term> terms;

    public Expression(){
        terms = new ArrayList<Term>();
    }
    public Expression(List<Term> terms) {
        this.terms = terms;
    }
    public boolean addTerm(Term term){
        return terms.add(term);
    }

    // 给定x，计算所有项的和
    public double evaluate(double x){
        double value = 0;
        for (Term term: terms) {
            value += term.evalute(x);
        }
        return value;
    }
    public static Expression fromString(String s) {
        Expression expression = new Expression();
        String[] terms = s.split("\\+");
        for (String term : terms){
            expression.addTerm(Term.fromString(term));
        }
        return expression;
    }
    @Override
    public String toString(){
        StringBuilder builder = new StringBuilder();
        int i;
        for (i=0; i<terms.size()-1; i++){
            builder.append(terms.get(i)).append(" + ");
        }
        builder.append(terms.get(i));
        return builder.toString();
    }
}


/**
 * 这里定义了整个函数coefficient * Math.pow(x, exponent);
 */
class Term {
    double coefficient;
    double exponent;

    Term(){
        coefficient = exponent = 0;
    }

    Term(double coefficient, double exponent){
        this.coefficient = coefficient;
        this.exponent = exponent;
    }

    // 解析给定的方程项
    public static Term fromString(String term) {
        double coefficient = 1;
        double exponent = 0;
        String[] splits = term.split("x", -1);
        if(splits.length > 0){
            String coefficientString = splits[0].trim();
            if(!(coefficientString.length() <= 0)){
                coefficient = Double.parseDouble(coefficientString);
            }
        }

        if(splits.length > 1) {
            exponent = 1;
            String exponentString = splits[1].trim();
            if(!(exponentString.length() <= 0)){
                exponent = Double.parseDouble(exponentString);
            }
        }
        return new Term(coefficient, exponent);
    }

    @Override
    public String toString(){
        return coefficient + "x^" + exponent;
    }

    public double evalute(double x) {
        return coefficient * Math.pow(x, exponent);
    }
}