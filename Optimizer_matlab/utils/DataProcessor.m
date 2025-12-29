function [code_list, score_vec, init_weight, df_score, df_index, exposure_mat, specific_vec, factor_cov_mat, factor_names, index_exposure_data] = DataProcessor(data_date, score_type, index_type, mode_type, dbc, df_st, df_stockuniverse)
% DataProcessor - 数据处理模块
% 处理股票数据、得分数据、风险暴露数据等
%
% 输入:
%   data_date - 数据日期
%   score_type - 得分类型
%   index_type - 指数类型
%   mode_type - 模式类型
%   dbc - 数据库连接器
%   df_st - ST股票列表（稳定数据）
%   df_stockuniverse - 股票宇宙数据（稳定数据）
%
% 输出:
%   code_list - 股票代码列表
%   score_vec - 得分向量
%   init_weight - 初始权重
%   df_score - 得分数据表
%   df_index - 指数数据表
%   exposure_mat - 风险暴露矩阵
%   specific_vec - 特异性风险向量
%   factor_cov_mat - 因子协方差矩阵
%   factor_names - 因子名称列表
%   index_exposure_data - 指数风险暴露数据

    % 获取原始得分表
    df_score = dbc.score_withdraw(data_date, score_type);
    
    % 获取 index 成分股
    df_index = dbc.index_component_withdraw(data_date, index_type);
    
    % 获取 ST 列表（使用稳定数据）
    % --- 统一将代码转为 string，避免 cell vs double 类型比较导致警告 ---
    df_index_codes = string(df_index.code);
    if ~isempty(df_st) && ismember('code', df_st.Properties.VariableNames)
        st_codes = string(df_st.code);
    else
        st_codes = string.empty;
    end

    % 交集：index 成分股且非 ST（使用 string 进行集合运算）
    valid_codes = setdiff(df_index_codes, st_codes);
    code_list = df_index_codes(ismember(df_index_codes, valid_codes));

    % 按index成分股顺序筛选df_score（统一为 string 比较）
    [is_in, loc] = ismember(string(code_list), string(df_score.code));
    score_vec = nan(length(code_list), 1);
    if any(is_in)
        score_vec(is_in) = df_score.final_score(loc(is_in));
    end
    
    % 处理得分数据中的NaN值
    score_vec(isnan(score_vec)) = 0;

    % 获取权重股池数据（用于mode_type切换）
    [df_hs300, df_zz500, df_zz1000, df_zz2000, df_zzA500] = dbc.index_exposure_withdraw(data_date);

    % 根据mode_type处理df_score
    switch mode_type
        case 'mode_v1'
            % 原始得分表
            % df_score 保持不变
        case 'mode_v2'
            if isempty(df_hs300) || isempty(df_zz500)
                error('权重股数据缺失');
            else
                df_score = score_zz800_stockpool_processing(df_score, df_hs300, df_zz500, dbc, data_date);
            end
        case 'mode_v3'
            if isempty(df_hs300) || isempty(df_zz500) || isempty(df_zz1000)
                error('权重股数据缺失');
            else
                df_score = score_zz1800_stockpool_processing(df_score, df_hs300, df_zz500, df_zz1000, dbc, data_date);
            end
        case 'mode_v4'
            if isempty(df_hs300) || isempty(df_zz500) || isempty(df_zz1000) || isempty(df_zz2000)
                error('权重股数据缺失');
            else
                df_score = score_zz3800_stockpool_processing(df_score, df_hs300, df_zz500, df_zz1000, df_zz2000, dbc, data_date);
            end
        otherwise
            error(['there is no mode type: ', mode_type]);
    end

    % 导出 index_initial_weight.csv（顺序与index成分股一致，初始权重=0或index权重）
    init_weight = zeros(length(code_list), 1);
    if ismember('weight', df_index.Properties.VariableNames)
        % 使用 string 进行比较以避免类型不一致的警告
        [~, idx_in_index] = ismember(string(code_list), string(df_index.code));
        init_weight(:) = df_index.weight(idx_in_index);
    end

    % 获取股票风险暴露数据
    try
        % 只传 data_date，获取全量暴露表
        exposure_tbl = dbc.stock_factor_exposure_withdraw(data_date);
        % 按 code_list 顺序筛选和补齐
        [is_in, loc] = ismember(string(code_list), string(exposure_tbl.code));
        factor_names = exposure_tbl.Properties.VariableNames;
        factor_names(strcmp(factor_names, 'code')) = [];
        n_stock = length(code_list);
        n_factor = length(factor_names);
        exposure_mat = nan(n_stock, n_factor);
        % 有暴露的直接赋值
        exposure_mat(is_in, :) = table2array(exposure_tbl(loc(is_in), factor_names));
        % 处理风险暴露数据中的NaN值
        exposure_mat(isnan(exposure_mat)) = 0;
    catch
        exposure_mat = [];
        factor_names = {};
    end

    % 获取特异性风险数据
    try
        specific_tbl = dbc.factor_risk_withdraw(data_date);
        code_names = specific_tbl.Properties.VariableNames;
        code_list_str = cellstr(string(code_list));
        specific_vec = nan(length(code_list_str), 1);
        for ii = 1:length(code_list_str)
            code = code_list_str{ii};
            if ismember(code, code_names)
                specific_vec(ii) = specific_tbl{'specificrisk', code};
            end
        end
        % 处理特异性风险数据中的NaN值
        specific_vec(isnan(specific_vec)) = 0;
    catch
        specific_vec = [];
    end

    % 获取因子协方差矩阵
    try
        factor_cov_tbl = dbc.factor_cov_withdraw(data_date);
        factor_names_cov = factor_cov_tbl.Properties.VariableNames(3:end); % 列名
        row_names = factor_cov_tbl.factor_name; % 行名
        n = numel(factor_names_cov);
        factor_cov_mat = nan(n, n);
        for idx_row = 1:n
            row_idx = find(strcmp(row_names, factor_names_cov{idx_row}));
            if ~isempty(row_idx)
                row_vals = table2array(factor_cov_tbl(row_idx, factor_names_cov));
                factor_cov_mat(idx_row, :) = row_vals;
            end
        end
        % 处理因子协方差数据中的NaN值
        factor_cov_mat(isnan(factor_cov_mat)) = 0;
    catch
        factor_cov_mat = [];
    end

    % 获取指数风险暴露数据（使用之前已获取的数据）
    try
        switch index_type
            case {'沪深300','hs300'}
                df_idx = df_hs300;
            case {'中证500','zz500'}
                df_idx = df_zz500;
            case {'中证1000','zz1000'}
                df_idx = df_zz1000;
            case {'中证2000','zz2000'}
                df_idx = df_zz2000;
            case {'中证A500','zzA500'}
                df_idx = df_zzA500;
            otherwise
                df_idx = df_hs300;
        end
        index_exposure_data = df_idx;
    catch
        index_exposure_data = [];
    end
end
