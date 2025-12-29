function [upper_tbl, lower_tbl, style_list, industry_list, turnover_val, score_weight, sharpe_weight, constraint_mode, up_params, low_params] = ConstraintProcessor(portfolio_name, factor_constraint, portfolio_constraint)
% ConstraintProcessor - 约束处理模块
% 处理因子约束、权重约束等参数
%
% 输入:
%   portfolio_name - 投资组合名称
%   factor_constraint - 因子约束表
%   portfolio_constraint - 投资组合约束表
%
% 输出:
%   upper_tbl - 上界约束表
%   lower_tbl - 下界约束表
%   style_list - 风格因子列表
%   industry_list - 行业因子列表
%   turnover_val - 换手率约束值
%   score_weight - 得分权重
%   sharpe_weight - 夏普权重
%   constraint_mode - 约束模式
%   up_params - 上界参数
%   low_params - 下界参数

    % 获取约束列名
    upper_col = [portfolio_name '_upper'];
    lower_col = [portfolio_name '_lower'];
    
    if ismember(upper_col, factor_constraint.Properties.VariableNames) && ismember(lower_col, factor_constraint.Properties.VariableNames)
        upper_tbl = table(factor_constraint.factor_name, factor_constraint.(upper_col), 'VariableNames', {'factor_name', 'upper_bound'});
        lower_tbl = table(factor_constraint.factor_name, factor_constraint.(lower_col), 'VariableNames', {'factor_name', 'lower_bound'});

        % 提取 style_list 和 industry_list
        all_factors = factor_constraint.factor_name;
        is_industry = cellfun(@(x) any(regexp(x, '[\x4e00-\x9fff]')), all_factors); % 有中文
        is_style = ~is_industry & ~strcmp(all_factors, 'TE');
        style_list = all_factors(is_style);
        industry_list = all_factors(is_industry);
    else
        error('未找到 %s 的upper或lower列', portfolio_name);
    end

    % 获取因子名列
    fn_candidates = {'factor_name','factor_constraint'};
    fn_colname = '';
    for fn = fn_candidates
        if ismember(fn{1}, portfolio_constraint.Properties.VariableNames)
            fn_colname = fn{1};
            break;
        end
    end
    if isempty(fn_colname)
        fn_colname = portfolio_constraint.Properties.VariableNames{1}; 
    end

    % 获取换手率约束
    max_turnover_idx = strcmp(portfolio_constraint.(fn_colname), 'max_turnover');
    if any(max_turnover_idx)
        fn_col = find(strcmp(portfolio_constraint.Properties.VariableNames, fn_colname));
        value_cols = setdiff(1:width(portfolio_constraint), fn_col);
        
        % 查找与portfolio_name匹配的列
        portfolio_col_idx = find(strcmp(portfolio_constraint.Properties.VariableNames, portfolio_name));
        if ~isempty(portfolio_col_idx)
            turnover_val = string(portfolio_constraint{max_turnover_idx, portfolio_col_idx});
        else
            turnover_val = string(portfolio_constraint{max_turnover_idx, value_cols(1)});
        end
    else
        turnover_val = '';
    end

    % 获取目标权重
    score_idx = strcmp(portfolio_constraint.(fn_colname), 'score_weight');
    sharpe_idx = strcmp(portfolio_constraint.(fn_colname), 'sharpe_weight');
    if any(score_idx) && any(sharpe_idx)
        fn_col = find(strcmp(portfolio_constraint.Properties.VariableNames, fn_colname));
        value_cols = setdiff(1:width(portfolio_constraint), fn_col);
        
        % 查找与portfolio_name匹配的列
        portfolio_col_idx = find(strcmp(portfolio_constraint.Properties.VariableNames, portfolio_name));
        if ~isempty(portfolio_col_idx)
            score_weight = string(portfolio_constraint{score_idx, portfolio_col_idx});
            sharpe_weight = string(portfolio_constraint{sharpe_idx, portfolio_col_idx});
        else
            score_weight = string(portfolio_constraint{score_idx, value_cols(1)});
            sharpe_weight = string(portfolio_constraint{sharpe_idx, value_cols(1)});
        end
    else
        score_weight = '';
        sharpe_weight = '';
    end

    % 获取约束模式
    row_idx = strcmp(portfolio_constraint.(fn_colname), 'constraint_mode');
    if any(row_idx)
        value_cols = setdiff(1:width(portfolio_constraint), find(strcmp(portfolio_constraint.Properties.VariableNames, fn_colname)));
        
        % 查找与portfolio_name匹配的列
        portfolio_col_idx = find(strcmp(portfolio_constraint.Properties.VariableNames, portfolio_name));
        if ~isempty(portfolio_col_idx)
            vals = portfolio_constraint{row_idx, portfolio_col_idx};
        else
            vals = portfolio_constraint{row_idx, value_cols};
        end
        
        if iscell(vals)
            vals = vals(~cellfun(@isempty, vals));
        end
        if ~isempty(vals)
            if iscell(vals)
                constraint_mode = vals{1};
            else
                constraint_mode = vals;
            end
        else
            constraint_mode = '';
        end
    else
        constraint_mode = '';
    end

    % 获取分组上下界参数
    quantiles = [0.9 0.8 0.7 0.6 0.5 0.4 0.3 0.2 0];
    up_names = { ...
        'comp_1_9_up','comp_9_8_up','comp_8_7_up','comp_7_6_up','comp_6_5_up', ...
        'comp_5_4_up','comp_4_3_up','comp_3_2_up','comp_2_0_up'};
    lo_names = { ...
        'comp_1_9_lo','comp_9_8_lo','comp_8_7_lo','comp_7_6_lo','comp_6_5_lo', ...
        'comp_5_4_lo','comp_4_3_lo','comp_3_2_lo','comp_2_0_lo'};
    
    % 读取up/lo参数
    up_params = nan(1, numel(up_names));
    low_params = nan(1, numel(lo_names));
    
    % 查找与portfolio_name匹配的列
    portfolio_col_idx = find(strcmp(portfolio_constraint.Properties.VariableNames, portfolio_name));
    fn_col = find(strcmp(portfolio_constraint.Properties.VariableNames, fn_colname));
    value_cols = setdiff(1:width(portfolio_constraint), fn_col);
    
    for k = 1:numel(up_names)
        up_idx = strcmp(portfolio_constraint.(fn_colname), up_names{k});
        lo_idx = strcmp(portfolio_constraint.(fn_colname), lo_names{k});
        
        if any(up_idx)
            if ~isempty(portfolio_col_idx)
                up_params(k) = str2double(portfolio_constraint{up_idx, portfolio_col_idx});
            else
                up_params(k) = str2double(portfolio_constraint{up_idx, value_cols(1)});
            end
        end
        if any(lo_idx)
            if ~isempty(portfolio_col_idx)
                low_params(k) = str2double(portfolio_constraint{lo_idx, portfolio_col_idx});
            else
                low_params(k) = str2double(portfolio_constraint{lo_idx, value_cols(1)});
            end
        end
    end
end
