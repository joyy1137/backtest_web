function df_score = score_zz800_stockpool_processing(df_score, df_hs300, df_zz500, dbc, data_date)
% score_zz800_stockpool_processing - 中证800股票池评分处理
% 筛选沪深300和中证500股票池，并对评分进行标准化处理
%
% 输入:
%   df_score - 原始评分数据表
%   df_hs300 - 沪深300指数数据
%   df_zz500 - 中证500指数数据
%
% 输出:
%   df_score - 处理后的评分数据表

    % 通过数据库连接器获取指数成分股代码列表
    df_hs300_components = dbc.index_component_withdraw(data_date, 'hs300');
    df_zz500_components = dbc.index_component_withdraw(data_date, 'zz500');
    
    % 获取股票代码列表
    code_list_hs300 = df_hs300_components.code;
    code_list_zz500 = df_zz500_components.code;
    
    % 合并并去重
    code_list_final = unique([code_list_hs300; code_list_zz500]);
    
    % 筛选在股票池中的数据
    is_in_pool = ismember(df_score.code, code_list_final);
    df_score = df_score(is_in_pool, :);
    
    % 对final_score进行标准化
    final_scores = df_score.final_score;
    score_mean = mean(final_scores);
    score_std = std(final_scores);
    
    % 标准化处理
    df_score.final_score = (final_scores - score_mean) / score_std;
    
end