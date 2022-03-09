#ifndef IRODS_GENQUERY_UTILITIES_HPP
#define IRODS_GENQUERY_UTILITIES_HPP

#include "edge_property.hpp"

#include <boost/graph/adjacency_list.hpp>

#include <set>
#include <vector>
#include <iterator>
#include <string_view>
#include <algorithm>

namespace irods::experimental::genquery
{
    template <typename VertexType>
    using path_type = std::vector<VertexType>;

    template <typename VertexType>
    using paths_type = std::set<path_type<VertexType>>;

    template <typename GraphType, typename VertexType>
    auto compute_all_paths_from_source_helper(const GraphType& _graph,
                                              VertexType _src_vertex,
                                              paths_type<VertexType>& _paths,
                                              path_type<VertexType>& _path) -> void
    {
        if (const auto e = std::end(_path);
            _path.size() > 1 && std::find(std::begin(_path), e, _src_vertex) != e)
        {
            _paths.insert(_path);
            return;
        }

        _path.push_back(_src_vertex);

        for (auto [iter, last] = boost::adjacent_vertices(_src_vertex, _graph); iter != last; ++iter) {
            compute_all_paths_from_source_helper(_graph, *iter, _paths, _path);
        }

        _path.pop_back();
    } // compute_all_paths_from_source_helper

    template <typename GraphType, typename VertexType>
    auto compute_all_paths_from_source(const GraphType& _graph, VertexType _src_vertex)
        -> paths_type<VertexType>
    {
        paths_type<VertexType> paths;
        path_type<VertexType> path;
        compute_all_paths_from_source_helper(_graph, _src_vertex, paths, path);
        return paths;
    } // compute_all_paths_from_source

    template <typename VertexType, typename GraphType>
    auto to_table_joins(const paths_type<VertexType>& _paths, const GraphType& _graph)
        -> std::vector<std::vector<std::string_view>>
    {
        std::vector<std::vector<std::string_view>> joins;
        joins.reserve(_paths.size());

        std::for_each(std::begin(_paths), std::end(_paths), [&_graph, &joins](auto&& _p) {
            joins.emplace_back();

            for (decltype(_p.size()) i = 0; i < _p.size() - 1; ++i) {
                if (const auto [edge, exists] = boost::edge(_p[i], _p[i + 1], _graph); exists) {
                    joins.back().push_back(_graph[edge].sql_join_condition);
                }
            }
        });

        return joins;
    } // to_table_joins

    template <typename VertexType, typename Container>
    auto filter(const paths_type<VertexType>& _paths, const Container& _required_table_names)
        -> paths_type<VertexType>
    {
        paths_type<VertexType> subset;

        for (auto&& p : _paths) {
            // This was an attempt to see what GenQuery would produce if it was required to
            // only accept paths containing the exact number of tables.
            //if (p.size() != _required_table_names.size()) {
            //    continue;
            //}

            const auto pred = [&p](auto&& _table_name)
            {
                return std::find(std::begin(p), std::end(p), _table_name) != std::end(p);
            };

            if (std::all_of(std::begin(_required_table_names), std::end(_required_table_names), pred)) {
                subset.insert(p);
            }
        }

        return subset;
    } // filter
} // namespace irods::experimental::genquery

#endif // IRODS_GENQUERY_UTILITIES_HPP
