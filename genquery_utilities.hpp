#ifndef IRODS_GENQUERY_UTILITIES_HPP
#define IRODS_GENQUERY_UTILITIES_HPP

#include "edge_property.hpp"

#include <boost/graph/adjacency_list.hpp>

#include <vector>
#include <iterator>
#include <string_view>
#include <algorithm>
#include <optional>

namespace irods::experimental::genquery
{
    template <typename VertexType>
    using path_type = std::vector<VertexType>;

    template <typename VertexType>
    using paths_type = std::vector<path_type<VertexType>>;

    template <typename GraphType, typename VertexType>
    auto compute_all_paths_from_source_helper(const GraphType& _graph,
                                              VertexType _src_vertex,
                                              paths_type<VertexType>& _paths,
                                              path_type<VertexType>& _path) -> void
    {
        if (const auto e = std::end(_path);
            _path.size() > 1 && std::find(std::begin(_path), e, _src_vertex) != e)
        {
            _paths.push_back(_path);
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
                subset.push_back(p);
            }
        }

        return subset;
    } // filter

    template <typename VertexType>
    auto get_shortest_path(const paths_type<VertexType>& _paths)
        -> const std::optional<path_type<VertexType>>
    {
        if (_paths.empty()) {
            return std::nullopt;
        }

        const auto* shortest_path = &*std::begin(_paths);

        for (auto iter = std::next(std::begin(_paths)); iter != std::end(_paths); ++iter) {
            if (iter->size() < shortest_path->size()) {
                shortest_path = &*iter;
            }
        }

        return *shortest_path;
    } // get_shortest_path

    template <typename VertexType, typename GraphType, typename TableNames>
    auto to_table_joins(const path_type<VertexType>& _path,
                        const GraphType& _graph,
                        std::vector<std::string>& _tables,
                        const TableNames& _table_names)
        -> std::vector<std::string_view>
    {
        std::vector<std::string_view> joins;
        joins.reserve(_path.size() - 1);

        for (decltype(_path.size()) i = 0; i < _path.size() - 1; ++i) {
            if (const auto [edge, exists] = boost::edge(_path[i], _path[i + 1], _graph); exists) {
                _tables.push_back(_table_names[_path[i]]);
                _tables.push_back(_table_names[_path[i + 1]]);
                joins.push_back(_graph[edge].sql_join_condition);
            }
        }

        return joins;
    } // to_table_joins
} // namespace irods::experimental::genquery

#endif // IRODS_GENQUERY_UTILITIES_HPP
