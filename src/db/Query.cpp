#include <algorithm>
#include <db/Query.hpp>
#include <map>
#include <numeric>
#include <stdexcept>

using namespace db;

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
  // TODO: Implement this function
  //projection operation

  const TupleDesc &in_td = in.getTupleDesc();

  for (const auto &tuple : in) {

    std::vector<field_t> fields;

    for (const auto &field_name : field_names) {

      //get index of field name
      size_t field_index = in_td.index_of(field_name);

      //append field
      fields.push_back(tuple.get_field(field_index));

    }

    //create tuple from selected field
    Tuple projected_tuple(fields);

    //write tuple to output table
    out.insertTuple(projected_tuple);
  }
}

bool evaluatePredicate(const field_t &field, PredicateOp op, const field_t &value) {

  //helper function to evaluate the predicate operation

  switch (op) {
    case PredicateOp::EQ: return field == value;
    case PredicateOp::NE: return field != value;
    case PredicateOp::LT: return field < value;
    case PredicateOp::LE: return field <= value;
    case PredicateOp::GT: return field > value;
    case PredicateOp::GE: return field >= value;
  }
  return false;
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
  // TODO: Implement this function
  //filter operation

  const TupleDesc &in_td = in.getTupleDesc();

  for (const auto &tuple : in) {

    bool matches = true;

    for (const auto &predicate : pred) {

      size_t index = in_td.index_of(predicate.field_name);
      const field_t &field = tuple.get_field(index);

      //check if predicate evaluates to true

      if (!evaluatePredicate(field, predicate.op, predicate.value)) {
        matches = false;
        break;
      }
    }
    if (matches) {
      //insert matched tuple to output table
      out.insertTuple(tuple);
    }
  }
}

double to_double(const field_t &field) {

    //helper function to convert to double

    return std::visit([](auto &&value) -> double {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_arithmetic_v<T>) {
            return static_cast<double>(value);
        }
        else {
            throw std::invalid_argument("Non-numeric type");
        }
    }, field);
}


void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
    // TODO: Implement this function

    //to perform aggregate operations

    const TupleDesc &in_td = in.getTupleDesc();

    //group the fields
    std::map<field_t, std::vector<field_t>> groups;

    size_t agg_field_index = in_td.index_of(agg.field);


    std::optional<size_t> group_field_index = agg.group.has_value()
                                              ? std::make_optional(in_td.index_of(agg.group.value()))
                                              : std::nullopt;


    // Group tuples and collect values for aggregation
    for (const auto &tuple : in) {

        field_t key = group_field_index.has_value() ? tuple.get_field(group_field_index.value()) : field_t{};
        field_t value = tuple.get_field(agg_field_index);

        groups[key].push_back(value);
    }

    std::vector<field_t> output_fields;

    for (const auto &[group, values] : groups) {

        field_t result;

          switch (agg.op) {

              case AggregateOp::SUM: {
                  //SUM all values
                  //convert to int or double
                  bool all_integers = true;

                  //for integers
                  int int_sum = 0;

                  //for doubles
                  double double_sum = 0.0;

                  for (const auto &val : values) {

                      //add to total

                      std::visit([&](auto &&arg) {
                          using T = std::decay_t<decltype(arg)>;
                          if constexpr (std::is_same_v<T, int>) {
                              int_sum += arg;
                          }
                          else if constexpr (std::is_same_v<T, double>) {
                              all_integers = false;
                              double_sum += arg;
                          }
                      }, val);
                  }

                  if (all_integers) {
                      result = int_sum;
                  } else {
                      result = int_sum + double_sum;
                  }
                  break;
              }

            case AggregateOp::MAX: {
                //Get Max Value

                if (!values.empty()) {
                    result = *std::max_element(values.begin(), values.end(), [](const field_t &a, const field_t &b) { return to_double(a) < to_double(b); });
                } else {
                    result = field_t{};  // Default value
                }
                break;
            }


            case AggregateOp::AVG: {
                //Get Average of values

                double sum = std::accumulate(values.begin(), values.end(), 0.0,[](double acc, const field_t &val) { return acc + to_double(val); });
                result = static_cast<field_t>(sum / values.size());
                break;
            }

            case AggregateOp::MIN: {

                //Get Minimum
                if (!values.empty()) {
                    result = *std::min_element(values.begin(), values.end(),
                                               [](const field_t &a, const field_t &b) { return to_double(a) < to_double(b); });
                } else {
                    result = field_t{};
                }
                break;
            }

            case AggregateOp::COUNT: {
                //Count
                result = static_cast<int>(values.size());
                break;
            }
        }


        output_fields.clear();
        if (group_field_index.has_value()) {
            output_fields.push_back(group);
        }

        output_fields.push_back(result);  // Aggregate result

        //write to output table
        out.insertTuple(Tuple(output_fields));
    }
}


void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  // TODO: Implement this function

}

