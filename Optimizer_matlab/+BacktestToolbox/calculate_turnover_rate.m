function turnover_rate = calculate_turnover_rate(prev_weights, curr_weights)
    % 计算换手率
    % 输入:
    %   prev_weights - 前一日的权重数据表 (包含code和weight列)
    %   curr_weights - 当日的权重数据表 (包含code和weight列)
    % 输出:
    %   turnover_rate - 换手率
    
    % 确保两个表都有数据
    if isempty(prev_weights) || isempty(curr_weights)
        turnover_rate = 0;
        return;
    end
    
    % 获取所有股票代码的并集
    all_codes = union(prev_weights.code, curr_weights.code);
    
    % 创建权重映射
    prev_weight_map = containers.Map();
    curr_weight_map = containers.Map();
    
    % 填充前一日的权重映射
    for i = 1:height(prev_weights)
        code = prev_weights.code{i};
        weight = prev_weights.weight(i);
        prev_weight_map(code) = weight;
    end
    
    % 填充当日的权重映射
    for i = 1:height(curr_weights)
        code = curr_weights.code{i};
        weight = curr_weights.weight(i);
        curr_weight_map(code) = weight;
    end
    
    % 计算换手率
    total_turnover = 0;
    
    for i = 1:length(all_codes)
        code = all_codes{i};
        
        % 获取权重，如果不存在则为0
        if isKey(prev_weight_map, code)
            prev_weight = prev_weight_map(code);
        else
            prev_weight = 0;
        end
        
        if isKey(curr_weight_map, code)
            curr_weight = curr_weight_map(code);
        else
            curr_weight = 0;
        end
        
        % 计算权重变化的绝对值
        weight_change = abs(curr_weight - prev_weight);
        total_turnover = total_turnover + weight_change;
    end
    
    % 换手率 = 总权重变化
    turnover_rate = total_turnover;
    
    % 确保换手率在合理范围内
    turnover_rate = max(0, min(turnover_rate, 1)); % 限制在0-100%之间
end
