function weight_result = calculate_daily_weight_contribution(daily_weight, daily_scores, df_components, top_number)
    % 计算单日权重贡献分析
    % 输入:
    %   daily_weight - 当日投资组合权重
    %   daily_scores - 当日投资组合评分
    %   df_components - 指数成分股信息（包含code和weight）
    %   top_number - 前N只股票数量
    % 输出:
    %   weight_result - 权重贡献分析结果 [missing, top, component_1.0_0.8, component_0.8_0.6, component_0.6_0.4, component_0.4_0.2, component_0.2_0.0]
    
    try
        % 合并投资组合权重、评分和指数成分股权重（与Python版本一致）
        % 重命名指数成分股权重列为 component_weight
        df_components_renamed = df_components;
        df_components_renamed.Properties.VariableNames{'weight'} = 'component_weight';
        
        % 合并投资组合权重和指数成分股权重
        merged_weights = outerjoin(daily_weight, df_components_renamed, 'Keys', 'code', 'MergeKeys', true);
        
        % 合并评分数据
        merged_weights = outerjoin(merged_weights, daily_scores, 'Keys', 'code', 'MergeKeys', true);
        
        if isempty(merged_weights)
            weight_result = [];
            return;
        end
        
        % 填充缺失值
        merged_weights.weight(isnan(merged_weights.weight)) = 0;
        merged_weights.component_weight(isnan(merged_weights.component_weight)) = 0;
        merged_weights.score(isnan(merged_weights.score)) = NaN;  % 保持NaN用于missing判断
        
        % 计算权重差异
        merged_weights.weight_difference = merged_weights.weight - merged_weights.component_weight;
        
        % 按评分排序进行分位数划分（与Python版本一致：使用final_score排序）
        merged_weights = sortrows(merged_weights, 'score', 'descend');
        n_stocks = height(merged_weights);
        
        % 计算各分位数的权重贡献
        weight_result = zeros(1, 7);
        
        % missing
        missing_mask = isnan(merged_weights.score);
        if sum(missing_mask) > 0
            weight_result(1) = sum(merged_weights.weight_difference(missing_mask));
        end
        
        % 移除没有评分的股票，按评分排序（与Python版本一致）
        scored_data = merged_weights(~missing_mask, :);
        scored_data = sortrows(scored_data, 'score', 'descend');
        
        % top: 不在指数成分股中的股票，按评分排序取前top_number只
        non_component_mask = isnan(scored_data.component_weight) | (scored_data.component_weight == 0);
        non_component_data = scored_data(non_component_mask, :);
        
        if height(non_component_data) >= top_number
            top_data = non_component_data(1:top_number, :);
        else
            top_data = non_component_data;
        end
        
        if height(top_data) > 0
            weight_result(2) = sum(top_data.weight_difference);
        end
        
        % component: 在指数成分股中的股票，按评分排序进行分位数划分
        component_mask = ~isnan(scored_data.component_weight) & (scored_data.component_weight > 0);
        component_data = scored_data(component_mask, :);
        
        if height(component_data) > 0
            % 按评分排序
            component_data = sortrows(component_data, 'score', 'descend');
            
            % 计算5个分位数的权重贡献（与Python版本一致）
            for i = 1:5
                j = (i - 1) * 0.2;  % [0, 0.2, 0.4, 0.6, 0.8]
                k = 0.2 + (i - 1) * 0.2;  % [0.2, 0.4, 0.6, 0.8, 1.0]
                
                % 计算分位数阈值（与Python版本一致：使用final_score进行分位数划分）
                quantile_lower = quantile(component_data.score, 1 - k);  % [0.8, 0.6, 0.4, 0.2, 0.0]
                quantile_upper = quantile(component_data.score, 1 - j);  % [1.0, 0.8, 0.6, 0.4, 0.2]
                
                % 筛选该分位数的股票（与Python版本一致）
                quantile_mask = (component_data.score < quantile_upper) & (component_data.score >= quantile_lower);
                
                if sum(quantile_mask) > 0
                    weight_result(i + 2) = sum(component_data.weight_difference(quantile_mask));
                end
            end
        end
        
    catch ME
        fprintf('计算单日权重贡献时出错: %s', ME.message);
        weight_result = [];
    end
end
