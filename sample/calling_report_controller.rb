class Zila::Api::CallingReportController < Zila::Api::BaseController
  before_action :authenticate_request!
  include ActionView::Helpers::NumberHelper
  # Agent wrong Disposition Report of logged user assigned state call centers.
  def agent_wrong_d_report
    call_center_ids = params[:call_center_ids]
    unless call_center_ids.present?
      raise StandardError, "Please select Call center"
    end
    start_date = (Date.today - 1.day).beginning_of_day.to_s
    end_date = (Date.today).end_of_day.to_s

    start_date = params[:start_date]&.in_time_zone(ENV['TIME_ZONE'])&.beginning_of_day.to_s if params[:start_date].present?
    end_date = params[:end_date]&.in_time_zone(ENV['TIME_ZONE'])&.end_of_day.to_s if params[:end_date].present?

    call_status_rejection = CallStatusType.where(status: 'Rejection')&.ids&.join(',')
    call_status_success = CallStatusType.success&.id
    call_status_mismatch = CallStatusType.where(status: 'Mismatch')&.ids&.join(',')
    call_status_later = CallStatusType.find_by(name: 'Asked to call later')&.id
    call_status_cut = CallStatusType.find_by(name: 'Got cut abruptly')&.id
    call_status_not_connect = CallStatusType.find_by(name: 'Did not Connect')&.id

    sql = "with qr as (SELECT
count(*) as attempted_calls,
sum(CASE WHEN call_logs.call_duration < 15 AND call_logs.call_status_type_id = #{call_status_success}  THEN 1 ELSE 0 END) as w_m_success,
sum(CASE WHEN call_logs.call_duration IS null AND call_logs.call_status_type_id = #{call_status_success}  THEN 1 ELSE 0 END) as w_m_success_null,
sum(CASE WHEN call_logs.call_duration < 15 AND call_logs.call_status_type_id IN (#{call_status_mismatch}) THEN 1 ELSE 0 END) as w_m_mismatch,
sum(CASE WHEN call_logs.call_duration IS null AND call_logs.call_status_type_id IN (#{call_status_mismatch}) THEN 1 ELSE 0 END) as w_m_mismatch_null,
sum(CASE WHEN call_logs.call_duration < 10 AND call_logs.call_status_type_id IN (#{call_status_rejection}) THEN 1 ELSE 0 END) as w_m_rejection,
sum(CASE WHEN call_logs.call_status_type_id IN (#{call_status_rejection}) AND call_logs.call_duration IS null THEN 1 ELSE 0 END) as w_m_rejection_null,
sum(CASE WHEN call_logs.call_duration < 1 AND call_logs.call_status_type_id = #{call_status_later} THEN 1 ELSE 0 END) as w_m_ask_later,
sum(CASE WHEN call_logs.call_duration IS null AND call_logs.call_status_type_id = #{call_status_later} THEN 1 ELSE 0 END) as w_m_ask_later_null,
sum(CASE WHEN call_logs.call_duration < 1 AND call_logs.call_status_type_id = #{call_status_cut} THEN 1 ELSE 0 END) as w_m_got_cut,
sum(CASE WHEN call_logs.call_duration IS null AND call_logs.call_status_type_id = #{call_status_cut} THEN 1 ELSE 0 END) as w_m_got_cut_null,
sum(CASE WHEN call_logs.call_duration > 30 AND call_logs.call_status_type_id = #{call_status_not_connect} THEN 1 ELSE 0 END) as w_d_m_dnc,
call_centers.name as cc_name, users.name as agent_name, users.email as agent_email, call_logs.user_id
FROM call_logs
LEFT JOIN users ON users.id = call_logs.user_id
JOIN call_centers ON call_centers.id = call_logs.call_center_id
WHERE call_logs.call_center_id IN (#{call_center_ids}) AND call_logs.created_at BETWEEN '#{start_date}' AND '#{end_date}'
GROUP BY call_logs.call_center_id, call_centers.name, call_logs.user_id, users.email, users.name)
select  *, (w_m_success + w_m_mismatch + w_m_success_null + w_m_mismatch_null) as wm_success,
          (w_m_ask_later + w_m_got_cut + w_m_ask_later_null + w_m_got_cut_null) as w_m_gca,
          (w_m_rejection + w_m_rejection_null) as rejection,
        (w_m_success + w_m_success_null + w_m_mismatch + w_m_mismatch_null + w_m_rejection + w_m_rejection_null + w_m_ask_later + w_m_ask_later_null + w_m_got_cut + w_m_got_cut_null + w_d_m_dnc) as total_wasted_calls,
        ((round((w_m_success + w_m_success_null + w_m_mismatch + w_m_mismatch_null + w_m_rejection + w_m_rejection_null + w_m_ask_later + w_m_ask_later_null + w_m_got_cut + w_m_got_cut_null + w_d_m_dnc)) / attempted_calls) * 100) as t_w_calls_per
from qr
where attempted_calls > 0"

    data = []
    @attempted_calls_total = []
    @w_d_m_dnc_total = []
    @w_m_success_total = []
    @w_m_rejection_total = []
    @w_m_gca_total = []
    @total_wasted_calls_total = []
    @total_wasted_calls_total_per = []
    records_array = ActiveRecord::Base.connection.execute(sql)
    records_array.each do |record|
      get_total_obj(record)
      data << {
        cc_name: record['cc_name'] || '',
        agent_name: record['agent_name'] || '',
        agent_email: record['agent_email'] || '',
        attempted_calls: record['attempted_calls'] || '',
        w_m_success: record['wm_success'] || '',
        w_m_rejection: record['rejection'] || '',
        w_m_gca: record['w_m_gca'] || '',
        w_d_m_dnc: record['w_d_m_dnc'] || '',
        total_wasted_calls: record['total_wasted_calls'] || '',
        total_wasted_calls_percentage: number_with_precision(record['t_w_calls_per'], precision: 2) || ''
      }
    end

    total_obj = {
      total_attempted_calls_count: @attempted_calls_total&.sum,
      w_d_m_dnc_total: @w_d_m_dnc_total&.sum,
      w_m_success_total: @w_m_success_total&.sum,
      w_m_rejection_total: @w_m_rejection_total&.sum,
      w_m_gca_total: @w_m_gca_total&.sum,
      total_wasted_calls_total: @total_wasted_calls_total&.sum,
      total_wasted_calls_total_per: ((@total_wasted_calls_total&.sum.to_f / @attempted_calls_total&.sum.to_f) * 100).round(2)
    }

    render json: {success: true, message: 'Agent Wrong Disposition Report', data: data || [], total: total_obj}, status: :ok
  rescue => e
    render json: { message: e.message }, status: :bad_request
  end

  def get_total_obj(record)
    @attempted_calls_total << record['attempted_calls']
    @w_d_m_dnc_total << record['w_d_m_dnc']
    @w_m_success_total << record['wm_success']
    @w_m_gca_total << record['w_m_gca']
    @w_m_rejection_total << record['rejection']
    @total_wasted_calls_total << record['total_wasted_calls']
    @total_wasted_calls_total_per << record['t_w_calls_per']
  end


  def state_cc
    data = []
    records_array = CallCenter.where(country_state_id:params[:country_state_id])
    records_array.each do |record|
      data << {
        name: record['name'] || '',
        id: record['id'] || ''
      }
    end

    render json: {success: true, message: 'State wise list of call centers', data: data || []}, status: :ok
  rescue => e
    render json: { success: false, message: e.message }, status: :bad_request
  end


  # Hourly Data Report of logged User assigned State (call center wise)
  def hourly_performance_report

    call_center_ids = params[:call_center_ids]

    start_date = (Date.today).beginning_of_day.utc.to_s
    end_date = (Date.today).end_of_day.utc.to_s

    start_date = params[:date]&.in_time_zone(ENV['TIME_ZONE'])&.beginning_of_day&.utc.to_s if params[:date].present?
    end_date = params[:date]&.in_time_zone(ENV['TIME_ZONE'])&.end_of_day&.utc.to_s if params[:date].present?

    completed_call_status_ids = CallStatusType.completed_calls&.join(',')
    connected_call_status_ids = CallStatusType.connected_call_ids&.join(',')


    sql = "with qr as (SELECT users.name as a_name,
users.email as a_email,
count(distinct(call_logs.user_id)) as attendance_count,
count(call_logs.id) as attempted_count,
call_logs.call_center_id as cen_id,
sum(CASE WHEN call_logs.call_status_type_id IN (#{connected_call_status_ids}) THEN 1 ELSE 0 END) as calls_connected,
sum(CASE WHEN call_logs.call_status_type_id IN (#{completed_call_status_ids}) THEN 1 ELSE 0 END) as completed_calls,
sum(CASE WHEN call_logs.call_duration IS NOT NULL THEN call_logs.call_duration ELSE 0 END) as call_duration,
date_trunc('hour',call_logs.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') as hour_data
FROM users
LEFT JOIN call_logs ON call_logs.user_id = users.id
WHERE users.disabled = false AND call_logs.call_center_id IN (#{call_center_ids}) AND call_logs.created_at BETWEEN '#{start_date}' AND '#{end_date}'
GROUP BY call_logs.call_center_id, date_trunc('hour',call_logs.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), users.id)
select  *, (call_duration::float / 60) as call_talk_time
from qr
where attempted_count > 0"

    records_array = ActiveRecord::Base.connection.execute(sql)
    data = []
    records_array.each do |item|
      data << {
        a_name: item['a_name'] || '',
        a_email: item['a_email'] || '',
        cen_id: item['cen_id'] || '',
        attempted_count: item['attempted_count'] || '',
        connected_calls: item['calls_connected'] || '',
        completed_calls: item['completed_calls'] || '',
        hour_data: item['hour_data'].strftime("%I:%M %p") || '',
        call_talk_time: item['call_talk_time'] || '',
        c_attempted: 0,
        c_connected: 0,
        c_completed: 0,
        c_talk_time: 0
      }
    end

    data.each do |item|
      time_s = (Time.strptime(item[:hour_data], "%I:%M %p") - 1.hour).strftime("%I:%M %p");
      found_obj = data.select do |obj|
        obj[:a_email] == item[:a_email] && obj[:hour_data] == time_s
      end

      if found_obj.empty?
        item[:c_attempted] = item[:attempted_count]
        item[:c_connected] = item[:connected_calls]
        item[:c_completed] = item[:completed_calls]
        item[:c_talk_time] = item[:call_talk_time]
      else
        item[:c_attempted] = item[:attempted_count] - found_obj[0][:attempted_count]
        item[:c_connected] = item[:connected_calls] - found_obj[0][:connected_calls]
        item[:c_completed] = item[:completed_calls] - found_obj[0][:completed_calls]
        item[:c_talk_time] = item[:call_talk_time] - found_obj[0][:call_talk_time]
      end
    end

    data = data.sort_by { |item| Time.parse(item[:hour_data]) }

    grouped_data  = data.group_by{ |item| item[:cen_id]}
    result = grouped_data.map do |cen_id, records|
      {
        'center_id' => cen_id,
        'value' => records
      }
    end

    result = result.map do |center_data|
      center_data['value'] = center_data['value'].group_by { |record| record[:hour_data] }.map do |hour_data, records|
        {
          'time' => hour_data,
          'reports' => records,
          'length' => records.length
        }
      end
      center_data
    end
    render json: { success: true, message: 'Data fetched successful.', data: result || []}, status: :ok
  rescue => e
    render json: { success: false, message: 'Not fetched..!'}, status: :bad_request
  end




  # Detailed Hourly Data of single call center and single time slot
  def detailed_hourly_report
    call_center_id = params[:call_center_id]
    date = params[:date].in_time_zone(ENV['TIME_ZONE']).to_date
    time = params[:time].to_i

    start_date = date.beginning_of_day + (time-6).hour + 30.minute
    end_date = date.beginning_of_day + (time-5).hour + 30.minute - 1.second

    completed_call_status_ids = CallStatusType.completed_calls&.join(',')
    connected_call_status_ids = CallStatusType.connected_call_ids&.join(',')


    sql = "with qr as (SELECT users.name as a_name,
users.email as a_email,
count(distinct(call_logs.user_id)) as attendance_count,
count(call_logs.id) as attempted_count,
call_logs.call_center_id as cen_id,
sum(CASE WHEN call_logs.call_status_type_id IN (#{connected_call_status_ids}) THEN 1 ELSE 0 END) as call_connected,
sum(CASE WHEN call_logs.call_status_type_id IN (#{completed_call_status_ids}) THEN 1 ELSE 0 END) as completed_calls,
sum(CASE WHEN call_logs.call_duration IS NOT NULL THEN call_logs.call_duration ELSE 0 END) as call_duration,
date_trunc('hour',call_logs.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') as hour_data
FROM users
LEFT JOIN call_logs ON call_logs.user_id = users.id
WHERE users.disabled = false AND call_logs.call_center_id = #{call_center_id} AND call_logs.created_at BETWEEN '#{start_date}' AND '#{end_date}'
GROUP BY call_logs.call_center_id, date_trunc('hour',call_logs.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), users.id)
select  *, (call_duration::float / 60) as call_talk_time
from qr
where attempted_count > 0"


    records_array = ActiveRecord::Base.connection.execute(sql)
    data = []
    total = []
    sum_of_agents = 0
    sum_of_attempted_count = 0
    sum_of_completed_calls = 0
    sum_of_talk_time = 0
    sum_of_connected_calls = 0
    records_array.each do |item|
      data << {
        a_name: item['a_name'] || '',
        a_email: item['a_email'] || '',
        attendance_count: item['attendance_count'],
        attempted_count: item['attempted_count'] || '',
        connected_calls: item['call_connected'] || '',
        completed_calls: item['completed_calls'] || '',
        call_talk_time: item['call_talk_time'] || '',
      }
    end

    data.each do |item|
      sum_of_agents+=item[:attendance_count]
      sum_of_attempted_count+=item[:attempted_count]
      sum_of_completed_calls+=item[:completed_calls]
      sum_of_talk_time+=item[:call_talk_time]
      sum_of_connected_calls+=item[:connected_calls]
    end

    total << {
      sum_of_agents: sum_of_agents,
      sum_of_attempted_count: sum_of_attempted_count,
      sum_of_completed_calls: sum_of_completed_calls,
      sum_of_connected_calls: sum_of_connected_calls,
      sum_of_talk_time: sum_of_talk_time
    }

    render json: {success: true, message: 'Done', data: data || [], total: total || []}, status: :ok
  rescue => e
    render json: { success: false, message: 'Not fetched..!'}, status: :bad_request
  end





  # Hourly Report Data of assigned state all call centers (Hour wise)
  def average_hourly_report

    call_center_ids = params[:call_center_ids]

    start_date = (Date.today).beginning_of_day.utc.to_s
    end_date = (Date.today).end_of_day.utc.to_s

    start_date = params[:date]&.in_time_zone(ENV['TIME_ZONE'])&.beginning_of_day&.utc.to_s if params[:date].present?
    end_date = params[:date]&.in_time_zone(ENV['TIME_ZONE'])&.end_of_day&.utc.to_s if params[:date].present?

    completed_call_status_ids = CallStatusType.completed_calls&.join(',')
    connected_call_status_ids = CallStatusType.connected_call_ids&.join(',')


    sql = "with qr as (SELECT users.name as a_name,
count(distinct(call_logs.user_id)) as agent_on_call,
count(call_logs.id) as attempted_count,
call_logs.call_center_id as cs_id,
sum(CASE WHEN call_logs.call_status_type_id IN (#{connected_call_status_ids}) THEN 1 ELSE 0 END) as call_connected,
sum(CASE WHEN call_logs.call_status_type_id IN (#{completed_call_status_ids}) THEN 1 ELSE 0 END) as completed_calls,
sum(CASE WHEN call_logs.call_duration IS NOT NULL THEN call_logs.call_duration ELSE 0 END) as call_duration,
date_trunc('hour',call_logs.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') as hour_data
FROM users
LEFT JOIN call_logs ON call_logs.user_id = users.id
WHERE users.disabled = false AND call_logs.call_center_id IN (#{call_center_ids}) AND call_logs.created_at BETWEEN '#{start_date}' AND '#{end_date}'
GROUP BY call_logs.call_center_id, date_trunc('hour',call_logs.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), users.id)
select  *, (call_duration::float / 60) as call_talk_time
from qr
where attempted_count > 0"

    records_array = ActiveRecord::Base.connection.execute(sql)
    data = []
    records_array.each do |item|
      data << {
        agents_on_call: item['agent_on_call'] || '',
        attempted_count: item['attempted_count'] || '',
        connected_calls: item['call_connected'] || '',
        completed_calls: item['completed_calls'] || '',
        hour_data: item['hour_data'].strftime("%I:%M %p") || '',
        call_talk_time: item['call_talk_time'] || '',
      }
    end

    grouped_data  = data.group_by{ |item| item[:hour_data]}

    result = []

    grouped_data.each do |time_slot, entries|
      total_agents_on_call = 0
      total_attempted_count = 0
      total_completed_calls = 0
      total_connected_calls = 0
      total_talk_time = 0

      entries.each do |item|
        total_agents_on_call+=item[:agents_on_call]
        total_attempted_count+=item[:attempted_count]
        total_completed_calls+=item[:completed_calls]
        total_connected_calls+=item[:connected_calls]
        total_talk_time += item[:call_talk_time]
      end


      result << {
        agents_on_call: total_agents_on_call,
        attempted_count: total_attempted_count,
        completed_calls: total_completed_calls,
        connected_calls: total_connected_calls,
        call_talk_time: total_talk_time,
        hour_data: time_slot
      }
    end


    result = result.sort_by { |item| Time.parse(item[:hour_data]) }

    return_data = []

    if result.length > 0
      return_data << {
        agents_on_call: result[0][:agents_on_call],
        attempted_count: result[0][:attempted_count],
        completed_calls: result[0][:completed_calls],
        connected_calls: result[0][:connected_calls],
        call_talk_time: result[0][:call_talk_time],
        hour_data: result[0][:hour_data],
        avg_attempted: result[0][:attempted_count].to_f  / result[0][:agents_on_call],
        avg_connected: result[0][:connected_calls].to_f  / result[0][:agents_on_call],
        avg_completed: result[0][:completed_calls].to_f  / result[0][:agents_on_call],
        avg_talk_time: result[0][:call_talk_time].to_f  / result[0][:agents_on_call],
        per_attempted: result[0][:attempted_count].to_f  / result[0][:agents_on_call],
        per_connected: result[0][:connected_calls].to_f  / result[0][:agents_on_call],
        per_completed: result[0][:completed_calls].to_f  / result[0][:agents_on_call],
        h_connective: result[0][:connected_calls].to_f  / result[0][:attempted_count],
        h_complete: result[0][:completed_calls].to_f  / result[0][:attempted_count]
      }

      result.each_with_index do |item, index|
        if index > 0
          return_data << {
            agents_on_call: item[:agents_on_call],
            attempted_count: item[:attempted_count],
            completed_calls: item[:completed_calls],
            connected_calls: item[:connected_calls],
            call_talk_time: item[:call_talk_time],
            hour_data: item[:hour_data],
            avg_attempted: item[:attempted_count].to_f  / item[:agents_on_call],
            avg_connected: item[:connected_calls].to_f  / item[:agents_on_call],
            avg_completed: item[:completed_calls].to_f  / item[:agents_on_call],
            avg_talk_time: item[:call_talk_time].to_f  / item[:agents_on_call],
            per_attempted: (item[:attempted_count].to_f  / item[:agents_on_call]) - return_data[index-1][:avg_attempted],
            per_connected: (item[:connected_calls].to_f  / item[:agents_on_call]) - return_data[index-1][:avg_connected],
            per_completed: (item[:completed_calls].to_f  / item[:agents_on_call]) - return_data[index-1][:avg_completed],
            h_connective: ((item[:connected_calls].to_f   / item[:agents_on_call]) - return_data[index-1][:avg_connected]).to_f  /
              ((item[:attempted_count].to_f  / item[:agents_on_call]) - return_data[index-1][:avg_attempted]),
            h_complete: ((item[:completed_calls].to_f  / item[:agents_on_call]) - return_data[index-1][:avg_completed]).to_f  /
              ((item[:attempted_count].to_f  / item[:agents_on_call]) - return_data[index-1][:avg_attempted])
          }
        end
      end
    end

    render json: {success: true, message: 'Average Hourly fetched', data: return_data || []}, status: :ok
    rescue => e
      render json: {success: false, message: 'Average not fetched..'}, status: :bad_request
  end

end