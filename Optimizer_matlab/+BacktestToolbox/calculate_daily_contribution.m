function contribution_result = calculate_daily_contribution(daily_weight, daily_scores, daily_stock_data, df_components, index_type, top_number)
    % 计算单日的成分股贡献分析
    % 输入:
    %   daily_weight - 当日投资组合权重（从weight.csv读取）
    %   daily_scores - 当日投资组合评分（从Stock_score.csv读取）
    %   daily_stock_data - 当日股票收益率数据
    %   df_components - 指数成分股信息（包含code和weight）
    %   index_type - 指数类型
    %   top_number - 前N只股票数量
    % 输出:
    %   contribution_result - 贡献分析结果 [missing, top, component_1.0_0.8, component_0.8_0.6, component_0.6_0.4, component_0.4_0.2, component_0.2_0.0]
    
    try
        % 计算指数收益率 - 通过指数成分股的加权平均收益率
        % df_components 包含指数成分股的 code 和 weight
        % 合并指数成分股权重和股票收益率数据
        
        % 确保code列是字符串类型
        if iscell(df_components.code)
            df_components.code = string(df_components.code);
        end
        if iscell(daily_stock_data.code)
            daily_stock_data.code = string(daily_stock_data.code);
        end
        
        index_merged = innerjoin(df_components, daily_stock_data, 'Keys', 'code');
        
        if isempty(index_merged)
            error('未找到指数成分股的收益率数据');
        end
        
        % 计算加权平均收益率作为指数收益率
        index_return = sum(index_merged.weight .* index_merged.pct_chg);
        
        % 合并投资组合权重、评分和指数成分股权重（与Python版本一致）
        % 重命名指数成分股权重列为 component_weight
        df_components_renamed = df_components;
        df_components_renamed.Properties.VariableNames{'weight'} = 'component_weight';
        
        % 合并投资组合权重和指数成分股权重
        merged_weights = outerjoin(daily_weight, df_components_renamed, 'Keys', 'code', 'MergeKeys', true);
        
        % 合并评分数据
        merged_weights = outerjoin(merged_weights, daily_scores, 'Keys', 'code', 'MergeKeys', true);
        
        % 合并股票收益率数据
        merged_data = innerjoin(merged_weights, daily_stock_data, 'Keys', 'code');
        
        if isempty(merged_data)
            contribution_result = [];
            return;
        end
        
        % 填充缺失值
        merged_data.weight(isnan(merged_data.weight)) = 0;
        merged_data.component_weight(isnan(merged_data.component_weight)) = 0;
        merged_data.score(isnan(merged_data.score)) = NaN;  % 保持NaN用于missing判断
        merged_data.pct_chg(isnan(merged_data.pct_chg)) = 0;
        
        daily_excess_return = merged_data.pct_chg - index_return;
        
        % 第一天：简单的净值差值 + 1（与主回测一致）
        if height(merged_data) > 0
            first_day_excess = 1 + daily_excess_return(1);
            
            % 从第二天开始：累积超额净值
            if height(merged_data) > 1
                cumulative_excess = cumprod(1 + daily_excess_return(2:end));
                merged_data.excess_return = [first_day_excess; first_day_excess * cumulative_excess];
            else
                merged_data.excess_return = first_day_excess;
            end
        else
            merged_data.excess_return = [];
        end
        
        % 计算权重差异
        merged_data.weight_difference = merged_data.weight - merged_data.component_weight;
        
        % 按评分排序进行分位数划分
        merged_data = sortrows(merged_data, 'score', 'descend');
        n_stocks = height(merged_data);
        
        % 计算各分位数的贡献
        contribution_result = zeros(1, 7);
        
        % missing
        missing_mask = isnan(merged_data.score);
        if sum(missing_mask) > 0
            contribution_result(1) = sum(merged_data.excess_return(missing_mask) .* merged_data.weight_difference(missing_mask));
        end
        
        % 移除没有评分的股票，按评分排序（与Python版本一致）
        scored_data = merged_data(~missing_mask, :);
        scored_data = sortrows(scored_data, 'score', 'descend');
        
        % top: 不在指数成分股中的前top_number只股票（与Python版本一致）
        % 筛选出不在指数成分股中的股票
        non_component_mask = isnan(scored_data.component_weight) | (scored_data.component_weight == 0);
        non_component_data = scored_data(non_component_mask, :);
        
        if ~isempty(non_component_data)
            top_count = min(top_number, height(non_component_data));
            if top_count > 0
                contribution_result(2) = sum(non_component_data.excess_return(1:top_count) .* non_component_data.weight_difference(1:top_count));
            end
        end
        
        % component分位数
        % 筛选出在指数成分股中的股票
        component_mask = ~isnan(scored_data.component_weight) & (scored_data.component_weight > 0);
        component_data = scored_data(component_mask, :);
        
        if ~isempty(component_data)
            % 按评分排序
            component_data = sortrows(component_data, 'score', 'descend');
            
            % Python版本使用 for i in range(0, 10, 2)，即 [0, 2, 4, 6, 8]
            % 对应 j = i/10 = [0, 0.2, 0.4, 0.6, 0.8]
            % 对应 k = 0.2 + 0.1*i = [0.2, 0.4, 0.6, 0.8, 1.0]
            % 分位数范围：quantile_lower = 1-k, quantile_upper = 1-j
            
            for i = 1:5
                j = (i - 1) * 0.2;  % [0, 0.2, 0.4, 0.6, 0.8]
                k = 0.2 + (i - 1) * 0.2;  % [0.2, 0.4, 0.6, 0.8, 1.0]
                
                % 计算分位数阈值
                quantile_lower = quantile(component_data.score, 1 - k);  % [0.8, 0.6, 0.4, 0.2, 0.0]
                quantile_upper = quantile(component_data.score, 1 - j);  % [1.0, 0.8, 0.6, 0.4, 0.2]
                
                % 筛选该分位数的股票
                quantile_mask = (component_data.score < quantile_upper) & (component_data.score >= quantile_lower);
                
                if sum(quantile_mask) > 0
                    contribution_result(i + 2) = sum(component_data.excess_return(quantile_mask) .* component_data.weight_difference(quantile_mask));
                end
            end
        end
        
    catch ME
        fprintf('计算单日贡献时出错: %s\n', ME.message);
        contribution_result = [];
    end
end
