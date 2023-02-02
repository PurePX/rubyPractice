# coding: utf-8
class EdnRule

  unless (const_defined?(:CLAIMS_PROCESSOR_SYSTEM_USER))
    CLAIMS_PROCESSOR_SYSTEM_USER = SysUser.claims_processor_system_user
  end

  def self.rules_for_plan(plan, opts={})
    # Expected opts => :dos (date),
    #                  :include_change_set (integer, change_set id)
    adjudication_rule_opts = {:dos => opts[:dos], :change_set_id => opts[:include_change_set], :program_ids => opts[:program_ids]}
    rules = ClaimRuleDefinition.adjudication_rules(plan.id, adjudication_rule_opts)
    log_millis("EdnRule:rules_for_plan:adjudication_rules:" + adjudication_rule_opts.to_json)
    rules
  end

  def self.rules_for_plan_by_context(plan, opts={})
    cache_key = "ClaimRuleDefinition:rules_for_plan_by_context:#{plan.id}:" +
                "#{ClaimRuleDefinition.truncate_to_effective_date(opts[:dos] || Date.today)}:" +
                "#{opts[:change_set_id]}:#{opts[:include_change_set]}:#{opts[:program_ids].to_a.sort}:#{opts[:order]}:" +
                ClaimRuleDefinition.latest_change_time_string
    vlog { "Rules for plan by context key: #{cache_key}" }

    dt_global_cache(cache_key, :expires_in => 1.hour) do
      rules = rules_for_plan(plan,opts)
      rules_by_context = {}
      rules.each do |rule|
        rule.applicable_contexts.each do |context|
          rules_by_context[context] ||= []
          rules_by_context[context] << rule
        end
      end
      rules_by_context
    end
  end

  def initialize(edn, opts={})
    @edn = normalize_edn(edn.is_a?(String) ? EDN.read(edn) : edn)
    @label = opts[:label]
    @description = opts[:description].present? ? opts[:description] : ''
    EdnRule.rule_error "Rule must be hash; got #{edn}" unless @edn.is_a?(Hash)
  end

  def label
    @label
  end

  def edn
    @edn
  end

  def description
    @description
  end

  def to_s
    {:label => @label,
     :rule => @edn}.to_edn
  end

  def ==(other)
    !!(other.is_a?(EdnRule) && label == other.label && edn == other.edn)
  end

  class LazySet
    def self.parse(str)
     dt_global_cache("lazyset:parse:#{str}", :expires_in => 10.minutes) {
       if str.index(",").nil? && str.index("-").nil?
         parse_date(str) || str
       else
         res = LazySet.new
         str.split(/,/).each do |s|
           range = s.split(/\-/)
           if range.size == 1
             res.add_elt(s)
           elsif range.size == 2
             res.add_range(range[0],range[1])
           else
              EdnRule.rule_error "Unable to parse as LazySet: #{str}"
           end
         end
         if (res.supernumary_ranges + res.other_ranges).present? || res.elts.size > 1
           res
         else
           res.elts.first
         end
       end
     }
    end

    def initialize
      @elts = []
      @supernumary_ranges = []
      @other_ranges = []
    end

    def add_elt(elt)
      @elts << elt
    end

    def add_range(e1,e2)
      sup = [e1,e2].map{|e| e.size == 2 && e.split('')[1] == 's'}
      if sup.all?
        @supernumary_ranges << [e1,e2]
      elsif !sup.any?
        @other_ranges << [e1,e2]
      else
        EdnRule.rule_error "Unrecognized range from #{e1} to #{e2}"
      end
    end

    def include?(elt)
      return true if @elts.include?(elt)
      return false if elt.nil?
      ranges = (elt.is_a?(String) && elt.size == 2 && elt.split('')[1] == 's') ? @supernumary_ranges : @other_ranges
      ranges.each do |r|
        if [elt,r[0],r[1]].all?{|x| x[0..0] =~ /[0-9]/}
          return true if r[0].to_f <= elt.to_f && r[1].to_f >= elt.to_f
        elsif ![elt,r[0],r[1]].any?{|x| x[0..0] =~ /[0-9]/}
          return true if elt.length == r[0].length && (r[0] <= elt && r[1] >= elt)
        end
      end
      false
    end

    def elts
      @elts
    end

    def supernumary_ranges
      @supernumary_ranges
    end

    def other_ranges
      @other_ranges
    end

    def intersect?(other)
      @elts.each do |e1|
        other.elts.each do |e2|
          return true if e1 == e2
        end
      end
      @supernumary_ranges.each do |r1|
        other.supernumary_ranges.each do |r2|
          return true unless r1[0] > r2[1] || r2[0] > r1[1]
        end
      end
      @other_ranges.each do |r1|
        other.other_ranges.each do |r2|
          return true unless r1[0] > r2[1] || r2[0] > r1[1]
        end
      end
      return false
    end

    def to_edn
      {:elts => @elts, :super => @supernumary_ranges, :other => @other_ranges}.to_edn
    end

    def to_s
      to_edn
    end

    def method_missing(meth, *args, &block)
      EdnRule.rule_error "LazySet #{self.to_edn} called with op #{meth} and args #{args.to_edn}"
    end
  end

  def self.expr_eq(args)
    if args.size < 2
      true
    else
      val1 = eval_expr(args.first)
      args.rest.all? {|arg|
        val2 = eval_expr(arg)
        if val1.is_a?(LazySet) && val2.is_a?(LazySet)
          val1.intersect?(val2)
        elsif val1.is_a?(LazySet)
          val1.include?(val2) || val1.include?(val2.to_s)
        elsif val2.is_a?(LazySet)
          val2.include?(val1) || val2.include?(val1.to_s)
        else
          val1 == val2
        end
      }
    end
  end

  def self.identical?(args)
    val1 = eval_expr(args.first)
    args.rest.all? do |arg|
      val2 = eval_expr(arg)
      if val1.is_a?(LazySet) && val2.is_a?(LazySet)
        val1.elts.sort == val2.elts.sort
      else
        val1 == val2
      end
    end
  end

  def self.expr_comp(comp,args)
    if args.size < 2
      true
    else
      rule_error "Unsupported comparator: #{comp}" unless %w(< > <= >=).include?(comp)
      eval_args = args.map { |arg| eval_expr(arg) }
      unless eval_args.none? &:nil?
        rule_error "Comparison #{comp} does not make sense with NIL values in parameters: #{eval_args.map { |arg| "“" + arg.inspect + "” " }}"
      end
      val1 = eval_expr(eval_args.first)
      eval_args.rest.all? {|arg|
        val2 = eval_expr(arg)
        res = val1.send(comp,val2)
        val1 = val2
        res
      }
    end
  end

=begin
  ce = ClaimEntry.find(51139834)

  EdnRule.eval_condition_for_entry(ce, ["=", "cpt", "0120"]) # expect true
  EdnRule.eval_condition_for_entry(ce, ["=", "cpt", "0140"]) # expect false

  age_rule =
    {"when" => ["and", ["=", "cpt", "0120,0150,0330"],
                       ["or", ["<", "age", 3],
                              [">", "age", 20]]],
     "deny" => 62}
  age_condition = age_rule['when']

  EdnRule.eval_condition_for_entry(ce, age_condition) # expect false - insured is 18 years old

  with_binding("trace_edn_expression_evaluation", true, :verbose, true) {
    EdnRule.eval_expr(['=', "12", "6,7,8,9,10"]) # expect false because 12 not in 6-10.
  }

=end
  def self.eval_enrollment_period(args)
    arg = eval_expr(args.first)
    get_binding('enrollment_duration')
  end

  def self.eval_family_count(args)
    insured_data = eval_insured_data('family_count', args)
    insured_data[:target_date] = get_binding(:target_date)
    insured_data[:beg_date] = get_binding(:beg_date)
    insured_data[:insured_ids] = get_binding('edn_rule:insured').id
    related_insureds = get_binding('insured_relationships') or InsuredRelationship.find_effective_for_insureds insured_data
    if(related_insureds.nil?)
      return 1
    else
      return related_insureds.count + 1
    end
  end

  def self.eval_iowa_tier(args)
    begin
      unless (args.size == 1)
        rule_error "Instead of one argument, got #{args.count}"
      end
      service = eval_expr(args.first)
      unless service.is_a? ClaimEntry
        rule_error "Instead of a Claim Entry, got #{service}"
      end
      claim = service.get_claim or
        rule_error "Claim Entry has no claim/preauth/…"
      insured = claim.insured or
        rule_error "Could not identify any Insured for claim"

      claim.insured.find_benefit_tier(if claim.is_claim?
                                      service.consider_date
                                     else
                                       Date.today
                                      end)

    rescue RuntimeError => e
      rule_error "#{e}.\nUsage: iowa_tier function must be given a claim entry to consider (for its insured and date), not #{args.to_edn}"
    end
  end

  def self.eval_as_of_date(entity)
    case entity.class.name.to_sym
      when :ClaimEntry, :MemClaimEntry
        entity.dos or entity.claim.date_received
      else
       Date.today
    end
  end

  def self.eval_insured_data(operator, args)
    insured_data = {}
    rule_error "#{operator} requires exactly one arg: #{args.to_edn}" unless args.size == 1
    arg = eval_expr(args.first)
    unless arg.is_a?(ClaimEntry) || arg.is_a?(MemClaimEntry) || arg.is_a?(Insured)
      rule_error "#{operator} arg must be claim entry or insured, found #{arg.class.name}"
    end
    as_of_date = eval_as_of_date arg
    if arg.is_a?(ClaimEntry) || arg.is_a?(MemClaimEntry)
      insured = arg.get_claim.insured
      rule_error "Unable to determine effective date for #{arg.get_claim.claim_id}" unless as_of_date
      insured_data[:group_plan_id] = arg.get_claim.group_plan_id
    else
      insured = arg
      insured_data[:group_plan_id] = get_binding(:group_plan_id) || insured.get_plan(as_of_date).id
      insured_data[:group_plan] = GroupPlan.find_by_id(insured_data[:group_plan_id])
      insured_data[:group_id] = if get_binding(:group_id)
                                  get_binding(:group_id)
                                elsif insured_data[:group_plan_id].nil?
                                  nil
                                else
                                  group_id = GroupPlan.find(insured_data[:group_plan_id]).group_id
                                  if group_id && group_id > 0
                                    group_id
                                  else
                                    nil
                                  end
                                end
    end
    insured_data[:insured] = insured
    insured_data[:target_date] = as_of_date
    insured_data
  end

  def self.eval_age(insured, opts = { :edit_date => nil, :operator_name => 'age' })
    opts = { :edit_date => nil, :operator_name => 'age' }.merge opts
    insured_data = eval_insured_data "age", insured
    insured = insured_data[:insured]
    date = insured_data[:target_date]
    if opts[:edit_date]
      date = date.send opts[:edit_date]
    end
      insured && insured.age_in_years(date)
  end

  def self.count_entries(entry, condition, opts = {})
    qty_found = 0
    count_self = opts[:count_self]
    qty_needed = opts[:qty_needed]
    get_binding(:claim_history).each do |claim|
      break if qty_found >= qty_needed
      if claim.document_type == 0 || claim.id == entry.get_claim.id
        claim.each_sorted_entry do |e|
          break if qty_found >= qty_needed
          if count_self
            self_condition = true
          else
            self_condition = e.id != entry.id
          end
          if (self_condition && eval_condition_for_entry(e, condition) && claim.voided_at.nil? && e.voided_at.nil?)
#            vlog {"\r\ncount_entries: e.cpt_code=#{e.cpt_code} e.status=#{e.status} found condition=#{condition}"}
            qty_found += 1
          end
        end
      end
    end
    qty_found
  end

  def self.eval_expr(expr, opts={})
    if expr.nil?
      return nil
    end

    ex = expr
    expr_begin = Time.now

    op_codes  =  %w(=  and   or  cpt  cdt  dos  <  >   <=  >=  not=  age
                  age_at_first_of_month months same  cond same_or exists
                  anniversary_before   anniversary_after   start_of_year
                  not_exists      preauth_exists      not_preauth_exists
                  preauth_exists_with     found      fqhc?     not_found
                  is_in_case_management?       provider_has_medicaid_id?
                  facility_has_teledentistry?  has_cob?
                  has_tpl? has_dental_tpl? has_medical_tpl?
                  has_non_medical_tpl?
                  preauthorized    has_xrays has_xray    has_preop_xrays
                  has_preop_xray has_postop_xrays has_postop_xray
                  has_anesthesia_time_record has_pathology
                  has_note_type?    is_claim    is_claim?     is_preauth
                  is_preauth?   preauth_for   not    transmission_method
                  entry_id claim_id uncovered present blank dos provider
                  facility  quadrant area  arch surface  iowa_tier tooth
                  num_surfaces   is_emergency    remarks   date_received
                  provider_type   anesthesia_level
                  max_anesthesia_level       has_anesthesia_certificate?
                  has_anesthesia_type?                  place_of_service
                  facility_anesthesia_level
                  max_facility_anesthesia_level
                  has_facility_anesthesia_type?
                  has_facility_anesthesia_certificate?
                  facility_type sys_added_entry as_of_date
                  expiration_date behavior_management_form count_entries
                  exists_at_least  plan plan_type encounter_rate_applies
                  pended approved denied mailed rate_code
                  enrollment_days membership_attribute
                  family_count  enrollment_period  benefit_max   program
                  member_ethnicity qty cdt_is_ada? cdt_max_qty
                  remarks_only    provider_npi    provider_fdh_effective
                  contains_any?      pos_code       remarks_have_dpc_no?
                  rendered_by_mdh mdh_provider mdh_facility
                  billed reimbursable_fee is_out_of_network?
                  deductible_anniversary_before deductible_anniversary_after
                  payment_hold_code exactly_same identical intersects
                  is_chisholm case_management_type)

    key_ops = {
      'days' => {:req => [], :opt => []},
      'entry_id' => {:req => [], :opt => []},
      'weeks' => {:req => [], :opt => []},
      'months' => {:req => [], :opt => []},
      'years' => {:req => [], :opt => []},
      'expiration_date' => {:req => [], :opt => []},
      'dos' => {:req => [], :opt => ['+','-']},
      'date_received' => {:req => [], :opt => ['+','-']},
      'exceeds_benefit_limit_within' => {:req => ['of'], :opt => ['qty']}
    }
    res = if ex.is_a?(Hash)
      op = key_ops.keys.find_all{|o| ex.has_key?(o)}.first
      rule_error "No valid op code found in expression:\r\n#{ex.to_edn}" unless op
      arg1 = ex[op]
      kargs = ex.reject {|k,v| k == op}
      opconfig = key_ops[op]
      missing_reqs = opconfig[:req] - kargs.keys
      rule_error "Missing required args for #{op}: #{missing_reqs.to_edn}" if missing_reqs.present?
      extra_kargs = kargs.keys - opconfig[:req] - opconfig[:opt]
      rule_error "Unrecognized args for #{op}: #{extra_kargs.to_edn}" if extra_kargs.present?
      if %w(days weeks months years).include?(op)
        rule_error "#{op} expects an integer, got #{arg1.class.name}" unless arg1.is_a?(Integer)
        arg1.send(op)
      elsif ['dos', 'date_received'].include?(op)
        entry = eval_expr(arg1)
        assert_entry(entry, :op => op)

        date_result = nil
        if entry.get_claim.is_claim?
          if op == 'date_received'
            date_result = entry.get_claim.date_received
          else
            date_result = entry.dos || entry.get_claim.date_received
          end
        else
          date_result = entry.get_claim.date_received
        end

        if kargs['+']
          date_result = date_result + eval_expr(kargs['+'])
        end
        if kargs['-']
          date_result = date_result - eval_expr(kargs['-'])
        end
        date_result
      elsif op == 'expiration_date'
        entry = eval_expr(arg1)
        assert_entry(entry, :op => op)
        entry.get_claim.expiration_date
      elsif op == 'entry_id'
        entry = eval_expr(arg1)
        entry.id
      elsif op == 'claim_id'
        entry = eval_expr(arg1)
        entry.get_claim.id
      elsif op == 'exceeds_benefit_limit_within'
        timeframe = eval_expr(arg1)
        rule_error "#{op} expects timeframe, received #{arg1.to_edn} (#{timeframe.class.name})" unless timeframe.is_a?(ActiveSupport::Duration)
        qty_found = 0
        relevant_entries = []
        entry = get_binding("edn_rule:entry")
        current_dos = entry.get_claim.is_claim? ? entry.dos : entry.get_claim.date_received

        get_binding(:claim_history).each do |claim|
          if claim.document_type == 0
            claim.each_sorted_entry do |e|
              include_sorted_entry = (e.id.present? && entry.id.present?) ?
                ((e.claim.id.to_i != entry.claim.id.to_i) || (e.id.to_i < entry.id.to_i && e.claim.id.to_i == entry.claim.id.to_i)) :
                (e.id.to_i > entry.id.to_i)
              if !e.is_voided? && e.dos > (current_dos - timeframe) && (e.dos < current_dos + timeframe) &&
                 include_sorted_entry
                if(eval_condition_for_entry(e,kargs['of']))
                  relevant_entries << e
                end
              end
            end
          end
        end

        relevant_entries.group_by { |e| e.dos }.each do |dos, entries|
          entries_considered = entries.find_all { |e| (e.is_approved? || (e.is_unprocessed? && e.claim.id.to_i == entry.claim.id.to_i))}

          entries_denied_med_nec = entries.find_all do |e|
            e.denied? && e.claim_carcs.any? { |cc| cc.ineligible_code.is_medical_necessity? }
          end

          # We cannot count denied entries towards frequency
          # limitations unless they are permanent resolutions to oral
          # health care issues (e.g. tooth extractions). However, the
          # rule engine does not currently support this distinction so
          # additional rules are written to cover those cases.

          entries_denied_other = []

          # entries_denied_other = entries.find_all do |e|
          #   e.denied? && e.claim_carcs.none? { |cc| cc.ineligible_code.is_medical_necessity? }
          # end

          ben_lim_contribution = filter_approved_entries(entries_considered).size

          unless current_dos == dos
            ben_lim_contribution += entries_denied_med_nec.size
            ben_lim_contribution += (entries_denied_other.size > 0) ? 1 : 0
          end

          qty_found += ben_lim_contribution
        end

        qty_found >= (kargs['qty'] ? eval_expr(kargs['qty']) : 1)
      else
        rule_error "op #{op} not supported for hash expressions"
      end
    elsif ex.is_a?(String)
      if op_codes.include?(ex)
        if get_binding("edn_rule:entry")
          eval_expr([ex, get_binding("edn_rule:entry")])
        elsif get_binding("edn_rule:insured")
          eval_expr([ex, get_binding("edn_rule:insured")])
        elsif get_binding("edn_rule:gpi")
          eval_expr([ex, get_binding("edn_rule:gpi")])
        else
          rule_error "#{ex} with unavailable default argument"
        end
      else
        get_binding("edn_rule:" + ex) ||
          get_binding("edn_rules:found_entries",{})[ex] ||
          LazySet.parse(ex) ||
          ex
      end
    elsif ex.is_a?(Enumerable)
      rule_error "Can't evaluate empty array" if ex.empty?
      op = ex.first
      args = ex.rest
      rule_error "First element of array should be an op: #{op}; #{args.to_edn}" unless op_codes.include?(op)
      if op == '='
        expr_eq(args)
      elsif op == 'not='
        eval_expr(['not', (['='] + args)])
      elsif op == 'and'
        args.all? {|arg| eval_expr(arg)}
      elsif op == 'or'
        args.any? {|arg| eval_expr(arg)}
      elsif op == 'not'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        evaled = eval_expr(args.first)
        if evaled == true || evaled == false
          !evaled
        else
          rule_error "#{op} needs boolean arg, got #{evaled} (#{evaled.class.name}) #{args.first.to_edn}"
        end
      elsif op == 'cond'
        val = nil
        cond_vals = args.each_slice(2).to_a
        cond_vals.each do |cv|
          if eval_expr(cv[0])
            val = cv[1]
            break
          end
        end
        val
      elsif op == 'present'
        rule_error "#{op} requires at least one arg" if args.blank?
        args.all? {|arg|
          x = eval_expr(arg)
          if x.is_a?(String)
            x.strip.present?
          else
            x.present?
          end
        }
      elsif op == 'blank'
        rule_error "#{op} requires at least one arg" if args.blank?
        eval_expr(['not', (['present'] + args)])
      elsif %w(< > <= >=).include?(op)
        expr_comp(op,args)
      elsif ['preauthorized'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        dt_scoped_cache([:edn_eval_entry_preauthorization,arg.id]) {
          pa = arg.preauthorization
          !!(pa && pa[:status] == :approved)
        }
      elsif op == 'enrollment_days'
        case args.size
        when 1
          entry = get_binding("edn_rule:entry")
        when 2
          entry = args[1]
        else
          rule_error "#{op} requires backdays arg and optional entry; got #{args.inspect}"
        end
        backdays = eval_expr(args.first)
        entry.continuous_enrollment_days_since(backdays)
      elsif ['is_claim','is_claim?'].include?(op)
        entry = get_binding("edn_rule:entry")
        entry.get_claim.is_claim?
      elsif ['is_preauth','is_preauth?'].include?(op)
        entry = get_binding("edn_rule:entry")
        entry.get_claim.is_preauth?
      elsif op == 'transmission_method'
        entry = get_binding("edn_rule:entry")
        entry.get_claim.get_transmission_method_label
      elsif op == 'fqhc?'
        entry = get_binding("edn_rule:entry")
        entry.is_medicaid_reimbursable?
      elsif op == 'is_in_case_management?'
        entry = get_binding("edn_rule:entry")
        insured_data = eval_insured_data(op, args)
        insured = insured_data[:insured]
        insured.present? ? insured.is_in_case_management? : false
      elsif op == 'provider_npi'
        entry = get_binding("edn_rule:entry")
        prv = entry.get_claim.provider
        prv && prv.npi ? prv.npi : nil
      elsif op == 'provider_fdh_effective'
        entry = get_binding("edn_rule:entry")
        prv = entry.get_claim.provider
        prv ? prv.fdh_effective : nil
      elsif op == 'provider_has_medicaid_id?'
        entry = get_binding("edn_rule:entry")
        prv = entry.get_claim.provider
        if prv.present?
          if prv.has_medicaid_id?
            true
          else
            fac = entry.get_claim.provider_facility
            if fac.present?
              db_select("select * from provider_facilities_providers
                          where provider_id = #{sql_escape(prv.id)}
                            and provider_facility_id = #{sql_escape(fac.id)}
                            and ap_effective_date <= #{sql_escape(entry.consider_date)}
                            and coalesce(ap_termination_date,'infinity') >= #{sql_escape(entry.consider_date)}
                            and medicaid_id is not null and medicaid_id != ''").
              present?
            else
              false
            end
          end
        else
          false
        end
      elsif op == 'facility_has_teledentistry?'
        !!(get_binding("edn_rule:entry").try(:get_claim).try(:provider_facility).try(:has_teledentistry?))
      elsif op == 'encounter_rate_applies'
        entry = get_binding("edn_rule:entry")
        entry.get_claim.encounter_rate_applies?
      elsif op == 'preauth_for'
        entry = get_binding("edn_rule:entry")
        preauth_info = entry.get_preauth(:filter_expired => false)
        qty_found = 0
        if preauth_info[:preauth].present? && preauth_info[:entry].present?
          if(eval_condition_for_entry(preauth_info[:entry],args.first) &&
              preauth_info[:preauth].voided_at.nil? && preauth_info[:entry].voided_at.nil?)
            qty_found += 1
          end
        end
        qty_found >= 1
      elsif %w{has_xrays has_xray has_preop_xrays has_preop_xray has_postop_xrays has_postop_xray}.include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        !!arg.has_possible_radiograph?
      elsif ['has_anesthesia_time_record'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        !!arg.has_possible_anesthesia_record?
      elsif ['has_pathology'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        !!arg.has_pathology?
      elsif ['has_note_type?'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        entry = get_binding("edn_rule:entry")
        notes_types = args.first
        !!entry.get_claim.find_claim_note_types(notes_types).present?
      elsif ['is_emergency'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.get_claim.is_emergency?
      elsif ['dos', 'date_received', 'expiration_date'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        eval_expr({op => args.first})
      elsif ['plan'].include?(op)
        if Profile.include_feature?('program-determination')
          insured_data = eval_insured_data(op, args)
          insured_data[:group_plan_id]
        else
          rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
          arg = eval_expr(args.first)
          assert_entry(arg, :op => op)
          arg.get_claim.group_plan_id
        end
      elsif ['plan_type'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        insured_data = eval_insured_data(op, args)
        insured = insured_data[:insured]
        group_plan = insured_data[:group_plan]
        group_plan.plan_tp
      elsif ['cpt','cdt'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.cpt_code
      elsif ['provider'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.get_claim.provider_id
      elsif ['facility'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.get_claim.provider_facility_id
      elsif op == 'has_cob?'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        entry = eval_expr args.first
        assert_entry(entry, :op => op)
        entry.claim.has_cob?
      elsif op == 'has_tpl?'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        entry = eval_expr args.first
        assert_entry(entry, :op => op)
        entry.claim.insured and
          entry.claim.insured.has_cob(entry.dos).present?
      elsif op == 'has_dental_tpl?'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        entry = eval_expr args.first
        assert_entry(entry, :op => op)
        entry.claim.insured and
          entry.claim.insured.has_cob(entry.dos, :type => :dental).present?
      elsif op == 'has_medical_tpl?'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        entry = eval_expr args.first
        assert_entry(entry, :op => op)
        entry.claim.insured and
          entry.claim.insured.has_cob(entry.dos, :type => :medical).present?
      elsif op == 'has_non_medical_tpl?'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        entry = eval_expr args.first
        assert_entry(entry, :op => op)
        entry.claim.insured and
          entry.claim.insured.has_cob(entry.dos, :type => :non_medical).present?
      elsif op == 'remarks'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.has_possible_narrative? ? "remarks_placeholder" : nil
      elsif op == 'remarks_only'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.remarks ? arg.remarks : nil
      elsif op == 'remarks_have_dpc_no?'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        if arg.remarks.present?
          arg.remarks =~ /DPC(\d\d\d\d)D/ ? true : false
        else
          false
        end
      elsif op == 'rendered_by_mdh'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.rendered_by_mdh?.to_s
      elsif ['mdh_provider','mdh_facility'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)

        # find MDH as of DOS
        insured = arg.get_claim.insured
        if op == "mdh_provider"
          insured.ifa_provider_id(arg.consider_date)
        else
          insured.ifa_provider_facility_id(arg.consider_date)
        end
      elsif op == 'pos_code'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.get_claim.pos_code.blank? ? nil : arg.get_claim.pos_code
      elsif op == 'contains_any?'
        rule_error "#{op} requires two or more args: #{args.to_edn}" unless args.size > 1
        text_arg = eval_expr(args.first)
        text_arg.present? && !!args.rest.detect{|k| text_arg.to_s.upcase.index(k.upcase)}
      elsif op == 'behavior_management_form'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.behavior_management_form? ? "behavior_placeholder" : nil
      elsif op == 'provider_type'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        (arg.get_claim.provider && arg.get_claim.provider.provider_type) ? arg.get_claim.provider.provider_type.description : nil
      elsif op == 'facility_type'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        (arg.get_claim.provider_facility && arg.get_claim.provider_facility.provider_facility_type) ? arg.get_claim.provider_facility.provider_facility_type.description : nil
      elsif op == 'anesthesia_level'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        # This operator must return a " " (space) so that it can be compared with "1", or "2", ...
        # For example as in: ["<=" "1" anesthesia_level]
        anesthesia_level = arg.get_claim.provider ? arg.get_claim.provider.anesthesia_level_str : " "
        anesthesia_level ? anesthesia_level : " "
      elsif op == 'facility_anesthesia_level'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        # This operator must return a " " (space) so that it can be compared with "1", or "2", ...
        # For example as in: ["<=" "1" anesthesia_level]
        claim = arg.get_claim
        anesthesia_level = claim.provider_facility ? claim.provider_facility.get_actual_anesthesia_level(claim.effective_date) : " "
        anesthesia_level ? anesthesia_level : " "
      elsif op == 'max_anesthesia_level'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
#        state_id_for_group_plan = arg.get_claim.group_plan.state.id
        state_id_for_facility = arg.get_claim.provider_facility.postal_code.state.id
        claim_anesthesia_level = arg.get_claim.provider ?
          arg.get_claim.provider.maximum_anesthesia_level_for_state(state_id_for_facility, arg.get_claim.effective_date) : nil
        claim_anesthesia_level.present? && claim_anesthesia_level >= 0 ? claim_anesthesia_level.to_s : " "
      elsif op == 'max_facility_anesthesia_level'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        state_id_for_facility = arg.get_claim.provider_facility.postal_code.state.id
        claim_anesthesia_level = arg.get_claim.provider_facility ?
          arg.get_claim.provider_facility.maximum_anesthesia_level_for_state(state_id_for_facility, arg.get_claim.effective_date) : nil
        claim_anesthesia_level.present? && claim_anesthesia_level >= 0 ? claim_anesthesia_level.to_s : " "
      elsif op == 'has_anesthesia_certificate?'
        entry = get_binding("edn_rule:entry")
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        certificates = args.first.split(",")
        assert_entry(entry, :op => op)
        #
        # It is questionable to use the certificates for the state where the provider_facility is located
        # Using the state for the group_plan was more resonable
#        state_id_for_group_plan = entry.get_claim.group_plan.state.id
        #
        state_id_for_facility = entry.get_claim.try(:provider_facility).try(:postal_code).try(:state).id
        claim_has_certificate = entry.get_claim.provider ?
          entry.get_claim.provider.has_anesthesia_certificate_for_state?(certificates, state_id_for_facility, entry.get_claim.effective_date) :
          false
      elsif op == 'has_facility_anesthesia_certificate?'
        entry = get_binding("edn_rule:entry")
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        certificates = args.first.split(",")
        assert_entry(entry, :op => op)
        state_id_for_facility = entry.get_claim.try(:provider_facility).try(:postal_code).try(:state).id
        claim_has_certificate = entry.get_claim.provider_facility ?
          entry.get_claim.provider_facility.has_anesthesia_certificate_for_state?(certificates, state_id_for_facility, entry.get_claim.effective_date) :
          false
      elsif op == 'has_anesthesia_type?'
        entry = get_binding("edn_rule:entry")
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        types = args.first.split(",")
        assert_entry(entry, :op => op)
        #
        # It is questionable to use the certificates for the state where the provider_facility is located
        # Using the state for the group_plan was more resonable
#        state_id_for_group_plan = entry.get_claim.group_plan.state.id
        #
        state_id_for_facility = entry.get_claim.try(:provider_facility).try(:postal_code).try(:state).id
        claim_has_certificate = entry.get_claim.provider ?
          entry.get_claim.provider.has_anesthesia_type_for_state?(types, state_id_for_facility, entry.get_claim.effective_date) :
          false
      elsif op == 'has_facility_anesthesia_type?'
        entry = get_binding("edn_rule:entry")
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        types = args.first.split(",")
        assert_entry(entry, :op => op)
        state_id_for_facility = entry.get_claim.try(:provider_facility).try(:postal_code).try(:state).id
        claim_has_certificate = entry.get_claim.provider_facility ?
          entry.get_claim.provider_facility.has_anesthesia_type_for_state?(types, state_id_for_facility, entry.get_claim.effective_date) :
          false
      elsif ['tooth'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        teeth = arg.tooth_array.compact.uniq
        invalid_teeth = teeth.select { |tooth_id| !(VALID_AREA_IDS + VALID_TOOTH_IDS).include?(tooth_id) }
        if invalid_teeth.present?
          vlog { "Can't evaluate claim_entry##{arg.id} invalid tooth: #{invalid_teeth.join(',')}" }
          teeth = teeth - invalid_teeth
        end
        if teeth.blank?
          nil
        else
          eval_expr(teeth.join(','))
        end
      elsif op == 'place_of_service'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.get_claim.place_of_service
      elsif ['num_surface','num_surfaces'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        surfaces = arg.surface_array.compact.uniq
        surfaces.length
      elsif ['surface'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        surfaces = arg.surface_array.compact.uniq
        if surfaces.blank?
          nil
        else
          eval_expr(surfaces.join(','))
        end
      elsif ['quadrant','area'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        quads = arg.tooth_array.map{|tooth|
          if tooth == '10' || UR_QUADRANT.include?(tooth)
            '10'
          elsif tooth == '20' || UL_QUADRANT.include?(tooth)
            '20'
          elsif tooth == '30' || LL_QUADRANT.include?(tooth)
            '30'
          elsif tooth == '40' || LR_QUADRANT.include?(tooth)
            '40'
          elsif tooth == '01'
            '01'
          elsif tooth == '02'
            '02'
          else
            nil
          end
        }.compact.uniq
        if quads.empty?
          nil
        else
          eval_expr(quads.join(','))
        end
      elsif op == 'arch'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        quads = arg.tooth_array.map{|tooth|
          if tooth == '01' || UR_QUADRANT.include?(tooth) || UL_QUADRANT.include?(tooth)
            '01'
          elsif tooth == '02' || LL_QUADRANT.include?(tooth)|| LR_QUADRANT.include?(tooth)
            '02'
          else
            nil
          end
        }.compact.uniq
        if quads.empty?
          nil
        else
          eval_expr(quads.join(','))
        end
      elsif op == 'approved'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.status == 2
      elsif op == 'denied'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.status == 3
      elsif op == 'pended'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.status == 1
      elsif op == 'mailed'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.get_claim.was_mailed?
      elsif op == 'uncovered'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.get_reimbursable_fee.nil?
      elsif op == 'entry_id'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.id
      elsif op == 'claim_id'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.get_claim.id
      elsif op == 'as_of_date'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        eval_as_of_date(eval_expr(args.first))
      elsif op == 'anniversary_before' || op == 'anniversary_after'
        rule_error "#{op} requires at least one arg: #{args.to_edn}" if args.size < 1
        rule_error "#{op} requires no more than 2 args: #{args.to_edn}" if args.size > 2
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        claim_benefit_from = nil
        claim_benefit_thru = nil

        with_binding("edn_rule:entry", nil) {
          suppress(StandardError) do
            group_rider = arg.group_rider!
            if group_rider
              claim_benefit_from = group_rider.effective_date
              claim_benefit_thru = group_rider.termination_date
            end
          end
          claim_benefit_thru = (claim_benefit_from + 1.year - 1.day) if claim_benefit_from && claim_benefit_thru.blank?

          if claim_benefit_from.blank? && claim_benefit_thru.blank?
            claim_benefit_from, claim_benefit_thru, benefit_effective_date = arg.get_claim.get_plan_benefit_period(arg.dos)
          end
        }
        anniversary = (op == 'anniversary_before' ?
          claim_benefit_from :
          claim_benefit_thru)
      elsif op == 'deductible_anniversary_before' || op == 'deductible_anniversary_after'
        rule_error "#{op} requires at least one arg: #{args.to_edn}" if args.size < 1
        rule_error "#{op} requires no more than 2 args: #{args.to_edn}" if args.size > 2
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        insured = arg.get_claim.insured
        insured_benefit_period = nil
        with_binding("edn_rule:entry", nil) do
          insured_benefit_period = Insured.deductible_plan_benefit_period(insured.id, arg.get_claim.group_plan_id, arg.dos)
        end
        anniversary = if op == 'deductible_anniversary_before'
                        insured_benefit_period[:deductible_benefit_from]
                      else
                        insured_benefit_period[:deductible_benefit_thru]
                      end
      elsif op == 'start_of_year'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        entry_dos = arg.consider_date
        Date.new(entry_dos.year,1,1)
      elsif op == 'age_at_first_of_month'
        case args.count
        when 1
          date = Date.today
        when 2
          date = args[1]
        else
          rule_error "#{op} must have one arg (insured/claim) for “today” or two with date specified, but got #{args.count}: #{args.inspect}"
        end
        eval_age args, :edit_date => :beginning_of_month, :operator_name => op
      elsif op == 'age'
        eval_age args
      elsif op == 'rate_code'
        if !Profile.include_feature?('program-determination')
          rule_error "#{op} requires feature program-determination"
        end
        insured_data = eval_insured_data(op, args)
        insured = insured_data[:insured]
        attributes = retrieve_or_set_binding("edn_rule:membership_attributes_#{insured.id}_#{insured_data[:target_date]}") do
          insured.membership_attributes(:group_plan_id => insured_data[:group_plan_id],
                                        :as_of => insured_data[:target_date]) if insured
        end
        rate_code = attributes && attributes["rate-code"] ? attributes["rate-code"].to_s : 0
      elsif op == 'family_count'
        eval_family_count(args)
      elsif op == 'enrollment_period'
        eval_enrollment_period(args)
      elsif op == 'membership_attribute'
        case args.size
          when 1
            entity = get_binding("edn_rule:entry")
          when 2
            entity = args[1]
          else
            rule_error "#{op} requires membership attribute code arg and optional entry; got #{args.inspect}"
        end
        entity = get_binding("edn_rule:entry") if entity.nil?
        entity = get_binding("edn_rule:insured") if entity.nil?
        insured_data = eval_insured_data(op, [entity])
        insured = insured_data[:insured]
        requested_attributes = args.first.present? ? args.first.split(",") : []
        attributes = retrieve_or_set_binding("edn_rule:membership_attributes_#{insured.id}_#{insured_data[:target_date]}") do
          insured.membership_attributes(:group_plan_id => insured_data[:group_plan_id],
                                                     :as_of => insured_data[:target_date]) if insured
        end
        attribute_value = check_attribute_value(requested_attributes, attributes, args)

        if entity.is_a?(ClaimEntry)
          effective_date = entity.claim.effective_date
          if attributes.blank? || attribute_value.nil?
            group_rider = entity.group_rider! rescue nil
            attributes = JSON.parse(group_rider.details) rescue nil
            attribute_value = check_attribute_value(requested_attributes, attributes, args)
          end
          if attributes.blank? && insured.present? || attribute_value.nil?
            insured_program = get_binding('program')
            insured_program = insured.program unless insured_program
            attributes = EntityAttribute.find_attributes_for_entity(insured_program, effective_date)
            attribute_value = check_attribute_value(requested_attributes, attributes, args)
          end
          if attributes.blank? && insured_data[:group_plan_id].present? || attribute_value.nil?
            group_plan = GroupPlan.find(insured_data[:group_plan_id]) rescue nil
            attributes = EntityAttribute.find_attributes_for_entity(group_plan, effective_date)
            attribute_value = check_attribute_value(requested_attributes, attributes, args)
          end
        end
        # For some reason, it's necessary to return 'false' explicitly
        if attribute_value.present?
          return true if attribute_value.to_s.downcase == 'true'
          return false if attribute_value.to_s.downcase == 'false'

          attribute_value
        else
          false
        end
      elsif op == 'member_ethnicity'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        insured = arg.get_claim.insured
        enthnicity = insured ? insured.ethnicity : ''
      elsif op == 'qty'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        quantity = arg.qty ? arg.qty : 1
      elsif op == 'cdt_is_ada?'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        is_ada = arg.cpt ? arg.cpt.is_ada? : false
      elsif op == 'cdt_max_qty'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        quantity = arg.cpt && arg.cpt.max_qty ? arg.cpt.max_qty : 1
      elsif op == 'benefit_max'
        entry = get_binding("edn_rule:entry")
        benefit_max = entry.claim.get_plan_benefit_max
        if benefit_max.present? && benefit_max > 0
          benefit_max
        else
          Float::MAX
        end
      elsif ['program'].include?(op)
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        arg = eval_expr(args.first)
        assert_entry(arg, :op => op)
        arg.get_claim.program.present? ? arg.get_claim.program.id : 0
      elsif op == 'iowa_tier'
        eval_iowa_tier(args)
      elsif op == 'same'
        eval_expr(['and'] + args.map{|prop| ['=', [prop, 'entry'], [prop, 'main']]})
      elsif op == 'intersects'
        expr_eq(args)
      elsif op == 'identical'
        identical?(args)
      elsif op == 'exactly_same'
        eval_expr(['and'] + args.map{|prop| ['identical', [prop, 'entry'], [prop, 'main']]})
      elsif op == 'same_or'
        eval_expr(['or'] + args.map{|prop| ['=', [prop, 'entry'], [prop, 'main']]})
      elsif ['not_exists','not_found'].include?(op)
        eval_expr(['not', (['exists'] + args)])
      elsif op == 'exists'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        qty_found = 0
        entry = get_binding("edn_rule:entry")
        condition = args[0]
        qty_found = count_entries(entry, condition, :qty_needed => 1)
        qty_found >= 1
      elsif op == 'count_entries'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        entry = get_binding("edn_rule:entry")
        condition = args[0]
        qty_found = count_entries(entry, condition, :count_self => true, :qty_needed => Float::MAX)
      elsif op == 'exists_at_least'
        rule_error "#{op} requires exactly two args: #{args.to_edn}" unless args.size == 2
        qty_needed = args[0].to_i
        entry = get_binding("edn_rule:entry")
        condition = args[1]
        qty_found = count_entries(entry, condition, :count_self => false, :qty_needed => qty_needed)
        qty_found >= qty_needed
      elsif ['found'].include?(op)
        rule_error "#{op} requires exactly two args: #{args.to_edn}" unless args.size == 2
        sym = args.first
        rule_error "First argument to #{op} should be symbol for binding: #{sym}" unless sym.is_a?(String)
        cond = args.second
        qty_found = 0
        entry = get_binding("edn_rule:entry")
        get_binding(:claim_history).each do |claim|
          break if qty_found > 0
          if claim.document_type == 0 || claim.id == entry.get_claim.id
            claim.each_sorted_entry do |e|
              break if qty_found > 0
              with_binding("edn_rule:" + sym, e) {
                if (e.id != entry.id && eval_condition_for_entry(e,cond) && claim.voided_at.nil? && e.voided_at.nil?)
                  qty_found = 1
                  get_binding("edn_rules:found_entries")[sym] = e
                end
              }
            end
          end
        end
        qty_found >= 1
      elsif op == 'sys_added_entry'
        entry = get_binding("edn_rule:entry")
        entry.added_by_systems?
      elsif ['not_preauth_exists'].include?(op)
        eval_expr(['not', (['preauth_exists'] + args)])
      elsif op == 'preauth_exists'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        qty_found = 0
        entry = get_binding("edn_rule:entry")
        get_binding(:preauth_history).each do |preauth|
          break if qty_found >= 1
          preauth.each_sorted_entry do |e|
            break if qty_found >= 1
            if(eval_condition_for_entry(e,args.first) && preauth.voided_at.nil? && e.voided_at.nil?)
              qty_found += 1
            end
          end
        end
        qty_found >= 1
      elsif op == 'preauth_exists_with'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        entry = get_binding("edn_rule:entry")
        eval_expr(['preauth_exists', ['and', args.first,

                                      #
                                      # TODO: Scope rules don't yet allow this:
                                      #    ['same','cpt'],['same','tooth'],['same','surface']
                                      #
                                      ['=', ['cpt',entry], ['cpt','entry']],
                                      ['=', ['tooth',entry], ['tooth','entry']],
                                      ['=', ['surface',entry], ['surface','entry']],
                                      'approved',
                                      'mailed',
                                      ['<=', ['dos', entry], ['expiration_date', 'entry']]]])

      elsif op == 'billed'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        entry = eval_expr(args.first)
        assert_entry(entry, :op => op)
        entry.amount_claim.to_f.to_s
      elsif op == 'reimbursable_fee'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" unless args.size == 1
        entry = eval_expr(args.first)
        assert_entry(entry, :op => op)
        entry.get_reimbursable_fee.present? ? entry.get_reimbursable_fee.amount.to_f.to_s : "0.0"
      elsif op == 'is_out_of_network?'
        entry = get_binding("edn_rule:entry")
        entry.get_claim.is_oon?
      elsif op == 'payment_hold_code'
        entry = get_binding("edn_rule:entry")
        entry.get_claim.provider.present? ? entry.get_claim.provider.payment_hold_code : nil
      elsif op == 'is_chisholm'
        entry = get_binding("edn_rule:entry")
        entry.claim.chisholm_member?
      elsif op == 'case_management_type'
        rule_error "#{op} requires exactly one arg: #{args.to_edn}" if args.size != 1
        required_cm_type = eval_expr(args.first)
        rule_error "#{op} requires exactly one string arg: #{args.to_edn}" if !required_cm_type.is_a?(String)
        entry = get_binding("edn_rule:entry")
        entry.claim.cm_cases.map { |cmc| cmc[:case_type].first }.include?(required_cm_type)
      else

        rule_error "Unsupported op: #{op}"
      end
    else
      ex
    end
    if get_binding("trace_edn_expression_evaluation")
      if res.class == ClaimEntry
        vlog {"#{ex.to_edn} => #{res.to_edn}[of claim #{res.claim.claim_id}]"}
      else
        vlog {"#{ex.to_edn} => #{res.to_edn}"}
      end
    end

    expr_time = Time.now - expr_begin
    if expr_time > 0.5
      vlog {"\r\nSpent #{expr_time} seconds on this expression: #{expr.to_edn}\r\n\r\n"}
    end

    res
  end

# RAILS_ENV=production ruby script/test_reprocess_claim.rb claims=1140014488452

  def self.eval_condition_for_entry(entry,condition,opts={})
    with_binding("edn_rule:entry", entry) {
      eval_expr(condition)
    }
  end

  def self.eval_condition_for_insured(insured,condition,opts={})
    with_binding("edn_rule:insured", insured) {
      with_binding("edn_rule:insured_opts", opts) {
        result = eval_expr(condition)
        EdnRule.on_claim_rule_execution_completed()
        result
      }
    }
  end

  def self.eval_condition_for_gpi(gpi,condition,opts={})
    with_binding("edn_rule:gpi", gpi) {
      with_binding("edn_rule:gpi_opts", opts) {
        result = eval_expr(condition)
        EdnRule.on_claim_rule_execution_completed()
        result
      }
    }
  end

  def self.on_claim_rule_execution_completed
    Thread.current.keys.grep(/edn_rule\:membership_attributes_/).each { |key| unset_binding(key) }
  end

=begin
  ruby script/test_reprocess_claim.rb claims=1140014488448

  {when [= cpt "2330,2331,2332,2335"]
   bundle_surfaces {
       bundlables [and [= cpt "2330,2331,2332,2335"]
                       [same dos]]
       result {
           dos main
           surfaces_to_cpt {1 "2330" 2 "2331" 3 "2332" 4 "2335"}
       }
       carc_on_bundleables 117
       carc_on_result_for_overpayment 101
  }}

  Similar to rule E0148: Bundling/Unbundling of Restorations
=end

  def self.filter_approved_entries(entries)
    filtered_entries = []
    if entries.count > 1 && entries.any?{|e| e.amount_cost > 0}
      entries.group_by {|e| e.cpt_code}.each do | cpt_code, claim_entries|
        if ((claim_entries.sum{|h|h[:amount_cost]}).abs < 0.05 )
          filtered_entries << claim_entries
        elsif (claim_entries.any?{|e| e.amount_cost < 0})
          filtered_entries << claim_entries.select{|e| e.claim_carcs.map(&:ineligible_code_id).include?(101) && e.amount_cost < 0 }
        end
      end
    end
    filtered_entries.present? ? entries - filtered_entries.flatten : entries
  end

  def self.check_action_keys(action, details, required_keys, optional_keys)
    required_keys.each do |k|
      rule_error "Action #{action} requires #{k}: #{details.to_edn}" unless details.has_key?(k)
    end
    unexpected_keys = details.keys - required_keys - optional_keys
    if unexpected_keys.present?
      rule_error "Action #{action} received unexpected keys #{unexpected_keys.map(&:to_s).join(',')}: #{details.to_edn}"
    end
  end

  def self.check_attribute_value(requested_attributes, attributes, args)
    if requested_attributes.size == 0
      attribute_value = nil
    elsif requested_attributes.size == 1
      attribute_value = attributes && args.first && attributes.key?(args.first) ? attributes[args.first] : nil
    else
      attribute_value = nil
      requested_attributes.each do |requested_attr|
        attribute_value = attributes && attributes.key?(requested_attr) ? attributes[requested_attr] : nil
        break if attribute_value.present?
      end
    end
    attribute_value
  end

  def self.look_for_result(entry, res_cpt, res_dos)
    soft_res = nil
    hard_res = nil
    #vlog {"looking for result code #{res_cpt}"}

    get_binding(:claim_history).each do |claim|
      if claim.document_type == 0 && claim.voided_at.nil?
        claim.each_sorted_entry do |e|
          if e.cpt_code == res_cpt && e.dos == res_dos && e.tooth_array.to_set == entry.tooth_array.to_set && e.voided_at.nil?
            if e.get_claim.paid_eob || (claim.claim_id =~ /[0-9]+A[0-9]+/ && claim.id != entry.claim_id)
              if e.approved?
                hard_res = e
              end
            else
              soft_res = e unless e.has_carc?(18)
            end
          end
        end
      end
    end
    return soft_res, hard_res
  end

  def self.send_bundling_recoup_email(claim_entry, recoup_amount, bundling_type)
    to = Profile.setting('bundling_recoup_notification_email')
    if to.present?
      claim_id = claim_entry.claim.claim_id
      subject = "The claim #{claim_id} needs recoup because of bundling"
      msg = "The claim with claim_id = #{claim_id} would have resulted in a negative payable amount because of #{bundling_type}.\n"
      msg += "\n  The amount to be recouped:   $#{recoup_amount.to_f.to_s}"
      msg += "\n  Claim date received:         #{claim_entry.claim.date_received}"
      msg += "\n  Bundle claim entry CDT:      #{claim_entry.cpt_code}"
      msg += "\n  Bundle claim entry tooths:   #{claim_entry.tooth_ids}"
      msg += "\n  Bundle claim entry surfaces: #{claim_entry.surface_ids}"
      send_email( to ,
        {:subject => subject,
         :msg => msg})
    end
  end

  def self.perform_surface_bundling(entry, details)
    required_keys = ['bundlables','result','carc_on_bundlables']
    optional_keys = ['carc_on_result','carc_on_result_for_overpayment','create_result']
    check_action_keys('bundle_surfaces', details, required_keys, optional_keys)

    #vlog {"Starting surface bundling process"}
    create_result = !!(!details.has_key?('create_result') || details['create_result'] == true)

    hard_bundlables = []
    soft_bundlables = []
    surfaces = []

    get_binding(:claim_history).
    each do |claim|
      if claim.document_type == 0 && claim.voided_at.nil?
        claim.each_sorted_entry do |e|
          # Crazy amount of logging because surface bundling not well tested yet.
          #vlog{"Comparing main entry #{entry.cpt_code}/#{entry.tooth_ids}/#{entry.surface_ids} with potential bundlable #{e.cpt_code}/#{e.tooth_ids}/#{e.surface_ids}"}
          increase_log_nesting {
          with_binding("trace_edn_expression_evaluation", true) {
          with_binding("edn_rule:bundlable", e) {
            app = (e.approved? || e.pended?) && !e.has_carc?(18)
            has_bundlable_carc = [details['carc_on_bundlables']].flatten.all? {|carc| e.has_carc?(carc)}
            same_tooth = e.tooth_array.to_set == entry.tooth_array.to_set
            #vlog{"app/status/has_carc/same_tooth = #{app}/#{e.status}/#{has_bundlable_carc}/#{same_tooth}"}
            if((app || has_bundlable_carc) &&
               same_tooth &&
               e.voided_at.nil? && e.created_by != CLAIMS_PROCESSOR_SYSTEM_USER.id &&
               eval_condition_for_entry(e,details['bundlables']))
              if e.get_claim.paid_eob || (claim.claim_id =~ /[0-9]+A[0-9]+/ && claim.id != entry.claim_id)
                hard_bundlables << e
              else
                soft_bundlables << e
              end
              surfaces = (surfaces + e.surface_array).uniq.sort
            end
          }
          }
          }
        end
      end
    end

    #vlog{"Found #{hard_bundlables.size} hard bundlables and #{soft_bundlables.size} soft bundlables."}
    #vlog{"Hard bundlables:"}
    #hard_bundlables.each do |hb|
    #  vlog{"    #{hb.cpt_code} status=#{hb.status} carcs=#{hb.claim_carcs.map{|cc| cc.ineligible_code_id.to_s}.join(',')}"}
    #end
    #vlog{"Soft bundlables:"}
    #soft_bundlables.each do |sb|
    #  vlog{"    #{sb.cpt_code} status=#{sb.status} carcs=#{sb.claim_carcs.map{|cc| cc.ineligible_code_id.to_s}.join(',')}"}
    #end

    if surfaces.empty?
      vlog{"Not bundling because found no surfaces"}
      return nil
    end

    res_dos = eval_expr(details['result']['dos'])
    if res_dos.is_a?(ClaimEntry) || res_dos.is_a?(MemClaimEntry)
      res_dos = res_dos.dos
    end
    rule_error "res_dos must be Date, found #{res_dos.class.name}: #{res_dos.to_edn}" unless res_dos.is_a?(Date)
    res_surface_to_cpt = details['result']['surfaces_to_cpt']
    rule_error "result->surface_to_cpt must be Hash, found #{res_surface_to_cpt.class.name}: #{res_surface_to_cpt.to_edn}" unless res_surface_to_cpt.is_a?(Hash)
    res_cpt = res_surface_to_cpt[surfaces.size]
    rule_error "result cpt for #{surfaces.size} surfaces must be String, found #{res_cpt.class.name}: #{res_cpt.to_edn}" unless res_cpt.is_a?(String)

    soft_res, hard_res = look_for_result(entry, res_cpt, res_dos)

    if (hard_bundlables + soft_bundlables).select{|ce| ce.created_by != CLAIMS_PROCESSOR_SYSTEM_USER.id}.blank?
      vlog{"Not bundling because all bundlables are system generated entries"}
      return nil
    end
    res = nil

    if hard_res
      res = hard_res
      #vlog{"surface_bunding found hard_res: #{res.cpt_code}/#{res.tooth_ids}/#{res.surface_ids}"}
    else
      already_paid = 0.0
      total_payable_without_bundling = 0.0
      total_billed = 0.0
      hard_bundlables.each do |ce|
        entry_payable = ce.amount_paid.to_f
        unless ce.get_claim.paid_eob
          fsl = ce.get_reimbursable_fee
          fee = fsl ? fsl.amount.to_f : 0.0
          entry_payable = (fee * ce.qty).abs * (ce.reversed? ? -1 : 1)
        end
        #vlog{"Found hard bundlable #{ce.cpt_code} with payable #{entry_payable}"}
        already_paid += entry_payable
        total_payable_without_bundling += entry_payable
        if ce.created_by == CLAIMS_PROCESSOR_SYSTEM_USER.id
          #vlog{"Not counting auto-generated bundle towards billed amounts"}
        else
          total_billed += ce.amount_claim.to_f
        end
      end
      soft_bundlables.each do |ce|
        fsl = ce.get_reimbursable_fee
        fee = fsl ? fsl.amount.to_f : 0.0
        entry_payable = (fee * ce.qty).abs * (ce.reversed? ? -1 : 1)
        #vlog{"Found soft bundlable #{ce.claim.claim_id}:#{ce.cpt_code} with payable #{entry_payable} and billed #{ce.amount_claim.to_f}"}
        #vlog{"                     provider #{ce.claim.provider_id} facility #{ce.claim.provider_facility_id}"}
        if ce.created_by == CLAIMS_PROCESSOR_SYSTEM_USER.id
          #vlog{"Not counting auto-generated bundle towards unbundled payable and billed amounts"}
        else
          total_payable_without_bundling += entry_payable
          total_billed += ce.amount_claim.to_f
        end
      end

      res_fee = nil
      if soft_res
        res = soft_res
        #vlog{"surface_bunding found soft_res: #{res.cpt_code}/#{res.tooth_ids}/#{res.surface_ids}"}
        fsl = res.apply_reimbursable_fee
        res_fee = fsl ? res.amount_cost.to_f : 0.0
        res.unit_amount_cost = res.unit_amount_benefit = [res_fee - already_paid, 0].max
        res.amount_cost = res.amount_benefit = [res_fee - already_paid, 0].max
        if soft_res.created_by == CLAIMS_PROCESSOR_SYSTEM_USER.id
          soft_res.unit_amount_claim = soft_res.amount_claim = [total_billed - already_paid,0].max
          soft_res.surface_ids = surfaces.to_s
          #vlog{"surface_bundling found that soft_res was auto-generated. set claim to #{soft_res.amount_claim} and surfaces to #{soft_res.surface_ids}"}
        end
        if res_fee - already_paid < 0
          EdnRule.send_bundling_recoup_email(res, res_fee - already_paid, "Surface Bundling")
        end
        soft_res.save! unless soft_res.claim_id == entry.claim_id
      elsif create_result
        res = ClaimEntry.new(
          :status => 2, :created_by => CLAIMS_PROCESSOR_SYSTEM_USER.id, :cpt_code => res_cpt, :dos => res_dos,
          :unit_amount_claim => [total_billed - already_paid,0].max, :claim_id => entry.claim_id,
          :surface_ids => surfaces.to_s, :tooth_ids => entry.tooth_ids, :qty => 1
        )
        fsl = res.apply_reimbursable_fee
        res_fee = fsl ? res.amount_cost.to_f : 0.0
        if entry.is_a?(ClaimEntry)
          res.unit_amount_cost = res.unit_amount_benefit = [res_fee - already_paid, 0].max
          res.save! unless res.claim_id == entry.claim_id
          if res_fee - already_paid < 0
            EdnRule.send_bundling_recoup_email(res, res_fee - already_paid, "Surface Bundling")
          end
        elsif entry.is_a?(MemClaimEntry)
          res_payable = [res_fee - already_paid, 0].max
          res_billed = [total_billed - already_paid,0].max
          res = MemClaimEntry.new(:claim => entry.claim, :cpt_code => res_cpt, :dos => res_dos, :status => 2,
                                  :unit_amount_claim => res_billed, :amount_claim => res_billed,
                                  :unit_amount_cost => res_payable, :unit_amount_benefit => res_payable,
                                  :amount_cost => res_payable, :amount_benefit => res_payable,
                                  :created_by => CLAIMS_PROCESSOR_SYSTEM_USER.id,
                                  :surface_ids => surfaces.to_s, :tooth_ids => entry.tooth_ids)
        else
          raise "Unexpected entry type: #{entry.class.name}"
        end
        entry.get_claim.claim_entries << res
        #vlog{"surface_bundling creating result: #{res.cpt_code}/#{res.tooth_ids}/#{res.surface_ids}"}
      else
        #vlog {"Not bundling - bundle does not exist and bundle creation turned off"}
        return nil
      end
      if details['carc_on_result']
        #vlog{"surface_bundling adding carc #{details['carc_on_result']} to result: #{res.cpt_code}/#{res.tooth_ids}/#{res.surface_ids}"}
        res.add_carc(details['carc_on_result'])
        res.save! unless res.claim_id == entry.claim_id
      end
      if details['carc_on_result_for_overpayment'] && already_paid > 0.005
        #vlog{"surface_bundling adding carc #{details['carc_on_result_for_overpayment']} to result for overpayment: " +
        #     "#{res.cpt_code}/#{res.tooth_ids}/#{res.surface_ids}"}
        res.add_carc(details['carc_on_result_for_overpayment'])
        res.save! unless res.claim_id == entry.claim_id
      end
    end
    soft_bundlables.each do |ce|
      if ce.id == res.id
        # Nothing, don't treat result like one of the bundlables
        #vlog{"surface_bundling skipping bundlable carc on result: #{ce.cpt_code}/#{ce.tooth_ids}/#{ce.surface_ids}"}
      elsif ce.created_by == CLAIMS_PROCESSOR_SYSTEM_USER.id
        #vlog{"Previously auto-generated #{ce.cpt_code}/#{ce.tooth_ids}/#{ce.surface_ids} as result, but not result anymore... deleting."}
        ce.deleted = 1
        ce.save! unless ce.claim_id == entry.claim_id
      else
        #vlog{"surface_bundling adding bundlable carc #{details['carc_on_bundlables']} to: #{ce.cpt_code}/#{ce.tooth_ids}/#{ce.surface_ids}"}
        ce.add_carc(details['carc_on_bundlables'], true)
        ce.remove_carc(14)
        ce.deny
        ce.save! unless ce.claim_id == entry.claim_id
      end
    end
    nil
  end

=begin

ruby script/tools/edn-rule-test.rb test row=19

{when [and [= cpt "0230"]
           [found exam [and [= cpt "0120,0130,0140"]
                                     ["<=" {dos exam} {dos main} {dos exam + {days 45}}]
                                     [= status approved]]]]

 bundle {bundlables [and [= cpt "0220,0230,0272,0274,0330"]
                                    ["<" dos {dos exam + {days 45}}]
                                    [or [same {dos exam}]
                                         [not= [{cpt bundlable} "0210,0220,0230"]]
                                         [not_exists [and [= cpt "2000-7999"]
                                               [same dos bundlable]
                                               [same_or [provider bundlable
                                                         facility bundlable]]]]]]

              result {cpt "0210",
                      dos exam},
              create_result true, # this is the default
              carc_on_bundlables 24,
              carc_on_result_without_deductions []
              carc_on_result_with_deductions [101,24]
              bundlables_paid_at_zero true}}

=end

  def self.perform_bundling(entry, details)

    #vlog{"Starting bundling #{entry.cpt_code} (#{entry.id}) ..."}
    #vlog{"  ... #{details['bundlables'].to_edn}"}

    required_keys = ['bundlables','result','carc_on_bundlables']
    optional_keys = ['carc_on_result','create_result']
    check_action_keys('bundle', details, required_keys, optional_keys)

    #vlog {"Starting bundling process"}
    create_result = !!(!details.has_key?('create_result') || details['create_result'] == true)

    hard_bundlables = []
    soft_bundlables = []

    get_binding(:claim_history).each do |claim|
      if claim.document_type == 0 && claim.voided_at.nil?
        claim.each_sorted_entry do |e|
          with_binding("edn_rule:bundlable", e) {
            if ((e.approved? || e.pended?) || [details['carc_on_bundlables']].flatten.all? {|carc| e.has_carc?(carc)}) &&
               e.voided_at.nil? && e.created_by != CLAIMS_PROCESSOR_SYSTEM_USER.id &&
               eval_condition_for_entry(e,details['bundlables'])
              if e.get_claim.paid_eob || (claim.claim_id =~ /[0-9]+A[0-9]+/ && claim.id != entry.claim_id)
                hard_bundlables << e
              else
                soft_bundlables << e
              end
            end
          }
        end
      end
    end

    bundlable_dates = (hard_bundlables + soft_bundlables).map(&:dos).sort
    bundlable_dates = [entry.dos] if bundlable_dates.blank?
    res_dos = nil
    with_binding("edn_rule:earliest_bundlable", bundlable_dates.first) {
      res_dos = eval_expr(details['result']['dos'])
    }
    if res_dos.is_a?(ClaimEntry) || res_dos.is_a?(MemClaimEntry)
      res_dos = res_dos.dos
    end
    rule_error "res_dos must be Date, found #{res_dos.class.name}: #{res_dos.to_edn}" unless res_dos.is_a?(Date)
    res_cpt = eval_expr(details['result']['cpt'] || details['result']['cdt'])
    rule_error "res_cpt must be String, found #{res_cpt.class.name}: #{res_cpt.to_edn}" unless res_cpt.is_a?(String)

    soft_res = nil
    hard_res = nil
    #vlog {"looking for result code #{res_cpt}"}
    get_binding(:claim_history).each do |claim|
      if claim.document_type == 0 && claim.voided_at.nil?
        claim.each_sorted_entry do |e|
          if e.cpt_code == res_cpt && (e.dos == res_dos || bundlable_dates.include?(e.dos)) && e.voided_at.nil?
            if e.get_claim.paid_eob || (claim.claim_id =~ /[0-9]+A[0-9]+/ && claim.id != entry.claim_id)
              if e.approved?
                hard_res = e
              end
            else
              soft_res = e unless e.has_carc?(18)
            end
          end
        end
      end
    end

    will_bundle = nil
    res = nil

    if hard_res
      will_bundle = true
    else
      already_paid = 0.0
      total_payable_without_bundling = 0.0
      total_billed_unpaid = 0.0
      total_billed_paid = 0.0
      hard_bundlables.each do |ce|
        entry_payable = ce.amount_paid.to_f
        unless ce.get_claim.paid_eob
          fsl = ce.get_reimbursable_fee
          fee = fsl ? fsl.amount.to_f : 0.0
          entry_payable = (fee * ce.qty).abs * (ce.reversed? ? -1 : 1)
        end
        #vlog{"Found hard bundlable #{ce.claim.claim_id}:#{ce.cpt_code} with payable #{entry_payable} and billed #{ce.amount_claim.to_f}"}
        #vlog{"                     provider #{ce.claim.provider_id} facility #{ce.claim.provider_facility_id}"}
        already_paid += entry_payable
        total_payable_without_bundling += entry_payable
        if ce.created_by == CLAIMS_PROCESSOR_SYSTEM_USER.id
          #vlog{"Not counting auto-generated bundle towards billed amounts"}
        else
          total_billed_paid += ce.amount_claim.to_f
        end
        #vlog{"  ... hard bundlable #{ce.cpt_code} (#{ce.id})"}
        #vlog{"     ... paid + #{entry_payable} => #{already_paid}"}
        #vlog{"     ... payable + #{entry_payable} => #{total_payable_without_bundling}"}
        #vlog{"     ... billed paid + #{ce.amount_claim.to_f} => #{total_billed_paid}"}
      end
      soft_bundlables.each do |ce|
        fsl = ce.get_reimbursable_fee
        fee = fsl ? fsl.amount.to_f : 0.0
        entry_payable = (fee * ce.qty).abs * (ce.reversed? ? -1 : 1)
        #vlog{"Found soft bundlable #{ce.claim.claim_id}:#{ce.cpt_code} with payable #{entry_payable} and billed #{ce.amount_claim.to_f}"}
        #vlog{"                     provider #{ce.claim.provider_id} facility #{ce.claim.provider_facility_id}"}
        if ce.created_by == CLAIMS_PROCESSOR_SYSTEM_USER.id
          #vlog{"Not counting auto-generated bundle towards unbundled payable and billed amounts"}
        else
          total_payable_without_bundling += entry_payable
          total_billed_unpaid += ce.amount_claim.to_f
        end
        #vlog{"  ... soft bundlable #{ce.cpt_code} (#{ce.id})"}
        #vlog{"     ... payable + #{entry_payable} => #{total_payable_without_bundling}"}
        #vlog{"     ... billed unpaid + #{ce.amount_claim.to_f} => #{total_billed_unpaid}"}
      end
      total_billed = total_billed_paid + total_billed_unpaid
      #vlog{"  billed #{total_billed} = #{total_billed_paid} + #{total_billed_unpaid}"}

      res_fee = nil
      if soft_res
        will_bundle = true
        res = soft_res
        #vlog{"Found soft result #{res.claim.claim_id}:#{res.cpt_code} with billed #{res.amount_claim.to_f}"}
        fsl = res.apply_reimbursable_fee
        res_fee = fsl ? res.amount_cost.to_f : 0.0
        res.unit_amount_cost = res.unit_amount_benefit = [res_fee - already_paid, 0].max
        res.amount_cost = res.amount_benefit = [res_fee - already_paid, 0].max
        if soft_res.created_by == CLAIMS_PROCESSOR_SYSTEM_USER.id
          soft_res.unit_amount_claim = soft_res.amount_claim = total_billed
        end
        if res_fee - already_paid < 0
          EdnRule.send_bundling_recoup_email(res, res_fee - already_paid, "Regular Bundling")
        end
        soft_res.save! unless soft_res.claim_id == entry.claim_id
        #vlog {"Updated entry #{res.cpt_code} billed:#{res.unit_amount_claim}:#{res.amount_claim}"}
        #vlog {"                              cost:#{res.unit_amount_cost}:#{res.amount_cost}"}
        #vlog {"                              benefit:#{res.unit_amount_benefit}:#{res.amount_benefit}"}
      elsif create_result
        res = ClaimEntry.new(
          :status => 2, :created_by => CLAIMS_PROCESSOR_SYSTEM_USER.id, :cpt_code => res_cpt, :dos => res_dos,
          :unit_amount_claim => total_billed, :claim_id => entry.claim_id, :qty => 1
        )
        fsl = res.apply_reimbursable_fee
        res_fee = fsl ? res.amount_cost.to_f : 0.0
        if res_fee < total_payable_without_bundling
          will_bundle = true
          if entry.is_a?(ClaimEntry)
            res.unit_amount_cost = res.unit_amount_benefit = [res_fee - already_paid, 0].max
            res.save! unless res.claim_id == entry.claim_id
            if res_fee - already_paid < 0
              EdnRule.send_bundling_recoup_email(res, res_fee - already_paid, "Regular Bundling")
            end
          elsif entry.is_a?(MemClaimEntry)
            res_payable = [res_fee - already_paid,0].max
            res = MemClaimEntry.new(:claim => entry.claim, :cpt_code => res_cpt, :dos => res_dos, :status => 2,
                                    :unit_amount_claim => total_billed, :amount_claim => total_billed,
                                    :unit_amount_cost => res_payable, :unit_amount_benefit => res_payable,
                                    :amount_cost => res_payable, :amount_benefit => res_payable,
                                    :created_by => CLAIMS_PROCESSOR_SYSTEM_USER.id)
          else
            raise "Unexpected entry type: #{entry.class.name}"
          end
          #vlog {"Created entry #{res.cpt_code} billed:#{res.unit_amount_claim}:#{res.amount_claim}"}
          #vlog {"                              cost:#{res.unit_amount_cost}:#{res.amount_cost}"}
          #vlog {"                              benefit:#{res.unit_amount_benefit}:#{res.amount_benefit}"}
          entry.get_claim.claim_entries << res
          #vlog {"Bundling: added result #{res.cpt_code} dos #{res.dos.to_s} with amount #{res.unit_amount_cost}"}
        else
          vlog {"Not bundling - doesn't save money (res_fee:#{res_fee}, total_payable_without_bundling:#{total_payable_without_bundling})"}
          return nil
        end
      else
        #vlog {"Not bundling - bundle does not exist and bundle creation turned off"}
        return nil
      end
      if will_bundle && details['carc_on_result']
        #vlog{"Adding CARC to result #{res.claim.claim_id}:#{res.cpt_code} with billed #{ce.amount_claim.to_f} carc #{details['carc_on_result']}"}
        res.add_carc(details['carc_on_result'])
        res.save! unless res.claim_id == entry.claim_id
      end
    end
    if will_bundle
      soft_bundlables.each do |ce|
        if ce != res
          #vlog{"Adding CARC to bundable #{ce.claim.claim_id}:#{ce.cpt_code} with billed #{ce.amount_claim.to_f} carc #{details['carc_on_bundlables']}"}
          ce.add_carc(details['carc_on_bundlables'], true)
          ce.remove_carc(14)
          ce.deny
        end
        ce.save! unless ce.claim_id == entry.claim_id
      end
    end
    nil
  end

  def self.perform_limit_lifetime_total(action, entry, details)
    required_keys = %w[contributors carc_on_partial carc_on_full action_on_full]
    optional_keys = %w[max_total action_on_partial abm ind_max fam_max]
    check_action_keys(action, details, required_keys, optional_keys)

    if details["action_on_partial"].present? && details["action_on_partial"] == "deny"
      rule_error "#{action} with {action_on_partial deny} is not allowed: #{details.to_edn}"
    end

    contributors_entries = find_contributors_entries(entry, details["contributors"])
    utilization = compute_plan_utilization(entry, contributors_entries)
    family_utilization = compute_plan_family_utilization(
        details["contributors"], {:entry => entry, :individual_contributors => contributors_entries}
    )
    begin
      remaining_benefit = calculate_lifetime_remaining_benefit(entry, utilization.merge!(family_utilization), details)
    rescue StandardError => e
      vlog { "Error: #{e}. Skipping #{action}"}
      return
    end

    apply_limit_total_to_claim_entry(entry, remaining_benefit, details)
  end

  def self.calculate_lifetime_remaining_benefit(entry, utilization, details)
    if %w(ind_max fam_max).any? { |key| details[key].present? }
      max_total = self.plan_benefit_max(entry, details['ind_max']) if details['ind_max'].present?
      family_max_total = self.plan_benefit_max(entry, details['fam_max']) if details['fam_max'].present?
    else
      max_total = entry.claim.get_plan_lifetime_benefit_max
    end
    max_total ||= 0.0
    family_max_total ||= 0.0

    return Float::MAX if max_total == 0.0 && family_max_total == 0.0

    max_total = Float::MAX if max_total == 0.0
    family_max_total = Float::MAX if family_max_total == 0.0
    utilization[:family_sum] = 0.0 if utilization[:family_sum].blank?

    if utilization[:sum] >= max_total ||
        utilization[:sum] >= family_max_total ||
        utilization[:family_sum] >= family_max_total

      0.0
    else
      [
        max_total - utilization[:sum],
        family_max_total - utilization[:sum],
        family_max_total - utilization[:family_sum]
      ].min.round(2)
    end
  end

  def self.perform_limit_total(action, entry, details)
    required_keys = %w[contributors carc_on_partial carc_on_full action_on_full]
    optional_keys = %w[max_total action_on_partial abm ind_max fam_max]
    check_action_keys(action, details, required_keys, optional_keys)

    if details["action_on_partial"].present? && details["action_on_partial"] == "deny"
      rule_error "#{action} with {action_on_partial deny} is not allowed: #{details.to_edn}"
    end

    contributors_entries = find_contributors_entries(entry, details["contributors"])
    utilization = compute_plan_utilization(entry, contributors_entries)
    family_utilization = compute_plan_family_utilization(
        details["contributors"], {:entry => entry, :individual_contributors => contributors_entries}
    )
    begin
      remaining_benefit = calculate_remaining_benefit(entry, utilization.merge!(family_utilization), details)
    rescue StandardError => e
      vlog { "Error: #{e}. Skipping #{action}"}
      return
    end

    apply_limit_total_to_claim_entry(entry, remaining_benefit, details)
  end

  def self.claim_entry_fee_for_service_in_abm(entry)
    reibursable_fee = nil
    use_fee_for_service_in_abm =
      EntityAttribute.find_attr_val_for_entity(entry.get_claim.group_plan,
      entry.get_claim.effective_date, "use_fee_for_service_in_abm_for_fqhc")
    if entry.is_medicaid_reimbursable? && use_fee_for_service_in_abm
      reibursable_fee = entry.get_reimbursable_fee
      reibursable_fee = reibursable_fee.present? ? reibursable_fee.amount.to_f : 0.0
    end

    return reibursable_fee
  end

  def self.calculate_remaining_benefit(entry, utilization, details)
    if %w(ind_max fam_max).any? { |key| details[key].present? }
      max_total = self.plan_benefit_max(entry, details['ind_max']) if details['ind_max'].present?
      family_max_total = self.plan_benefit_max(entry, details['fam_max']) if details['fam_max'].present?
    elsif details['max_total'].present?
      max_total = eval_expr(details['max_total'])
    else
      max_total = entry.claim.get_plan_benefit_max
      family_max_total = entry.claim.get_plan_family_benefit_max
    end
    max_total ||= 0.0
    family_max_total ||= 0.0

    return Float::MAX if max_total == 0.0 && family_max_total == 0.0

    max_total = Float::MAX if max_total == 0.0
    family_max_total = Float::MAX if family_max_total == 0.0
    utilization[:family_sum] = 0.0 if utilization[:family_sum].blank?

    if utilization[:sum] >= max_total ||
        utilization[:sum] >= family_max_total ||
        utilization[:family_sum] >= family_max_total

      0.0
    else
      [
        max_total - utilization[:sum],
        family_max_total - utilization[:sum],
        family_max_total - utilization[:family_sum]
      ].min.round(2)
    end
  end

  def self.plan_benefit_max(entry, detail)
    return nil if detail.blank?

    insured = entry.claim.insured
    effective_date = entry.claim.effective_date
    group_plan_id = entry.claim.group_plan_id
    ind_max_attribute = Float(detail) rescue detail
    case ind_max_attribute
    when Float # concrete value
      ind_max_attribute
    when String # field name in json
      group_rider = entry.group_rider! rescue nil
      group_rider_details = JSON.parse(group_rider.details) rescue nil
      if group_rider_details && group_rider_details.key?(ind_max_attribute)
        attribute = group_rider_details[ind_max_attribute].try(:to_f)
      end
      if attribute.blank? && insured.present?
        insured_program = insured.program
        attribute = EntityAttribute.find_attr_val_for_entity(insured_program, effective_date, ind_max_attribute)
      end
      if attribute.blank? && group_plan_id.present?
        group_plan = GroupPlan.find(group_plan_id) rescue nil
        attribute = EntityAttribute.find_attr_val_for_entity(group_plan, effective_date, ind_max_attribute)
      end

      attribute
    else
      nil
    end
  rescue JSON::ParserError, ActiveRecord::RecordNotFound
    return nil
  end

  def self.apply_limit_total_to_claim_entry(entry, remaining_benefit, details)
    if fee_for_service = claim_entry_fee_for_service_in_abm(entry)
      min_amount = fee_for_service
    else
      min_amount = [entry.amount_cost, entry.amount_claim].min
    end
    vlog { "Remaining benefit: #{remaining_benefit.to_f}" }

    if remaining_benefit.to_f.zero?
      entry.amount_cost = entry.unit_amount_cost = 0
      entry.exceeding_benefit += min_amount
      add_carcs_to_entry(entry, details['carc_on_full'])
      entry.apply_action(details['action_on_full'])
      entry.save!

      return
    end

    exceeding_benefit = 0
    amount_with_abm = calculate_amount_cost_with_abm(entry, remaining_benefit)
    exceeding_benefit = min_amount - amount_with_abm if min_amount.to_f > amount_with_abm.to_f
    exceeding_benefit = entry.exceeding_benefit + exceeding_benefit if entry.exceeding_benefit.to_f.abs > 0
    vlog { "Exceeding benefit: #{exceeding_benefit.to_f}" }
    return if amount_with_abm.to_f >= min_amount.to_f

    # If not using fee for service in the calculation for ABM (fee_for_service is nil), do save the
    # necessary amount cost fields in the claim entry, otherwise this is an FQHC ABM calculation
    # for which the amount cost fields should remain as is as the claim rules processing did leave them.
    if fee_for_service.blank?
      entry.unit_amount_cost = (amount_with_abm / entry.qty).abs
      entry.amount_cost = entry.unit_amount_cost * entry.qty
    end
    entry.exceeding_benefit = exceeding_benefit
    entry.add_carc(details['carc_on_partial'])
    entry.apply_action(details['action_on_partial'])
    entry.save!
  end

  def self.calculate_amount_cost_with_abm(entry, remaining_benefit)
    if fee_for_service = claim_entry_fee_for_service_in_abm(entry)
      min_amount = fee_for_service
    else
      min_amount = [entry.amount_cost.to_f.abs, entry.amount_claim.to_f.abs].min
    end
    if min_amount - entry.cob.to_f.abs - entry.deductible.to_f.abs -
      entry.copay.to_f.abs - entry.coinsurance.to_f.abs < remaining_benefit

      min_amount
    else
      remaining_benefit + entry.cob.to_f.abs + entry.deductible.to_f.abs + entry.copay.to_f.abs +
        entry.coinsurance.to_f.abs
    end
  end

  def self.compute_contributors_recover(entries_to_recover, entry_to_recover_from, rule_details)
    total_amount_to_recover = entries_to_recover.collect{|ce| ce.claim.paid_eob ?
       (ce.amount_paid.to_f) : (ce.unit_amount_cost.to_f * ce.qty)}.sum
    fsl = entry_to_recover_from.get_reimbursable_fee
    max_recoverable_amount = fsl ? fsl.amount.to_f : 0.0
    is_full_recoverable = total_amount_to_recover <= max_recoverable_amount
    if is_full_recoverable
      action_on_recover = rule_details["action_on_full"] || "approve"
      carc_on_recover = rule_details['carc_on_full']
    else
      action_on_recover = rule_details["action_on_partial"] || "approve"
      carc_on_recover = rule_details['carc_on_partial']
    end
    return {:is_full_recoverable => is_full_recoverable, :max_recoverable_fee => max_recoverable_amount,
      :total_amount_to_recover => total_amount_to_recover,
      :action_on_recover => action_on_recover, :carc_on_recover => carc_on_recover}
  end

  def self.perform_recoup(action, entry, details)
    required_keys = ['from_cdt', 'carc_on_partial','carc_on_full','action_on_full']
    optional_keys = ['action_on_partial']
    check_action_keys(action, details, required_keys, optional_keys)

    if details["action_on_partial"].present? && details["action_on_partial"] == "deny"
      rule_error "#{action} with {action_on_partial deny} is not allowed: #{details.to_edn}"
    end

    recoup_from_entries = find_contributors_entries(entry, details["from_cdt"])
    if (recoup_from_entries.blank?)
      vlog {"Action perform_recoup for #{entry.claim.claim_id} did not find contributors"}
      return "not_recouped"
    end

    recoup_results = compute_contributors_recover(recoup_from_entries, entry, details)
    carc_on_recoup = recoup_results[:carc_on_recover]

    recoup_from_entries.each do |recoup_entry|
      recoup_cpt = recoup_entry.cpt_code
      recoup_fee = recoup_entry.claim.paid_eob ?
        recoup_entry.amount_paid : recoup_entry.unit_amount_cost * recoup_entry.qty
      if ClaimEntry.rule_find(:insured => entry.claim.insured_id,
         :status => [2],
         :on_dos => recoup_entry.dos,
         :created_by => CLAIMS_PROCESSOR_SYSTEM_USER.id,
         :cdts => [recoup_cpt]).size == 0
        res = ClaimEntry.new(
          :status => 2, :created_by => CLAIMS_PROCESSOR_SYSTEM_USER.id, :cpt_code => recoup_cpt, :dos => recoup_entry.dos,
          :unit_amount_claim => recoup_entry.amount_claim, :claim_id => entry.claim_id, :qty => 1
        )
        if entry.is_a?(ClaimEntry)
          res.unit_amount_cost = res.unit_amount_benefit = -recoup_fee
          res.save!
        elsif entry.is_a?(MemClaimEntry)
          res_payable = -recoup_fee
          res = MemClaimEntry.new(:claim => entry.claim, :cpt_code => recoup_cpt, :dos => entry.dos, :status => 2,
                                  :unit_amount_claim => total_billed, :amount_claim => total_billed,
                                  :unit_amount_cost => res_payable, :unit_amount_benefit => res_payable,
                                  :amount_cost => recoup_entry.amount_claim, :amount_benefit => res_payable,
                                  :created_by => CLAIMS_PROCESSOR_SYSTEM_USER.id)
        else
          raise "Unexpected entry type: #{entry.class.name}"
        end
        add_carcs_to_entry(res, carc_on_recoup)
        vlog {"Recouped claim #{recoup_entry.claim.claim_id} with claim_entry #{recoup_entry.cpt_code}"}
        return "recouped"
      else
        vlog {"Action perform_recoup for #{entry.claim.claim_id}," +
          " contributor #{recoup_entry.claim.claim_id},#{recoup_cpt}, already recouped"}
        return "nothing_to_recoup"
      end
    end
  end

  def self.add_carcs_to_entry(claim_entry, carcs_data)
    if carcs_data && carcs_data.is_a?(Integer)
      claim_entry.add_carc(carcs_data, true)
    elsif carcs_data && carcs_data.is_a?(Array)
      carcs_data.each do |carc|
        claim_entry.add_carc(carc, false)
      end
    else
      rule_error "Could NOT apply CARCS to entry because they are not single number or array of numbers"
    end
  end

  def self.recover_entries(duplicated_claim, claim_data, details, action_on_reversed, carc_on_reversed)
    claim_entries_to_recoup = duplicated_claim.claim_entries.select{|ce|
      ce.cpt_code == claim_data[:cpt_code] && ce.tooth_ids == claim_data[:tooth_ids]}
    claim_entries_to_recoup.each do |claim_entry|
      claim_entry.unit_amount_cost = claim_entry.unit_amount_benefit =
        claim_entry.amount_cost = claim_entry.amount_benefit =
        claim_entry.copay = claim_entry.interest = claim_entry.cob = claim_entry.deductible = 0
      add_carcs_to_entry(claim_entry, details['carc_on_partial'])
      if action_on_reversed.present?
        claim_entry.send(action_on_reversed)
      else
        claim_entry.approve
      end
      claim_entry.created_by = CLAIMS_PROCESSOR_SYSTEM_USER.id
      claim_entry.save!
      add_carcs_to_entry(claim_entry, carc_on_reversed )
    end
  end

  def self.perform_reverse(action, entry, details)
    required_keys = ['from_cdt', 'carc_on_reverse', 'carc_on_full', 'carc_on_partial']
    optional_keys = [ 'action_on_full', 'action_on_partial']
    check_action_keys(action, details, required_keys, optional_keys)

    reverse_from_entries = find_contributors_entries(entry, details["from_cdt"])
    if (reverse_from_entries.blank?)
      vlog {"Action perform_reverse for #{entry.claim.claim_id} did not find contributors, already reversed"}
      return "not_reversed"
    end

    recover_results = compute_contributors_recover(reverse_from_entries, entry, details)
    action_on_reversed = recover_results[:action_on_recover]
    carc_on_reversed = recover_results[:carc_on_recover]

    claims_to_reverse = reverse_from_entries.collect{|ce| {:claim => ce.get_claim,
      :claim_entry => ce.id, :cpt_code => ce.cpt_code, :tooth_ids => ce.tooth_ids}}

    rule = get_binding("edn_rule")
    rule_text = rule ? "Rule #{rule.label}: " : ''
    rule_description = rule ? "#{rule.description}" : ''
    prefix = rule_text + entry.to_edn + " "
    claims_to_reverse.each do |claim_data|
      claim = claim_data[:claim]
      if not claim.is_reversed?
        reversed_claim = claim.reverse_me(CLAIMS_PROCESSOR_SYSTEM_USER.id)
        vlog {"#{prefix} reversed claim #{claim.claim_id} into claim #{reversed_claim.claim_id}"}
        reversed_claim.claim_entries.each do |reverse_ce|
          reverse_ce.created_by = CLAIMS_PROCESSOR_SYSTEM_USER.id
          reverse_ce.save!
          add_carcs_to_entry(reverse_ce, details['carc_on_reverse'] )
        end
        reversed_claim.finalize!(SysUser.system_user)
      end

      if not claim.is_duplicated?
        duplicated_claim = claim.duplicate_me(CLAIMS_PROCESSOR_SYSTEM_USER.id)
        vlog {"#{prefix} duplicated claim #{claim.claim_id} into claim #{duplicated_claim.claim_id}"}
        recover_entries(duplicated_claim, claim_data, details, action_on_reversed, carc_on_reversed)
        duplicated_claim.finalize!(SysUser.system_user)
        duplicated_claim.save!
        log_claim(duplicated_claim, {:include_entries => true, :verbose => true})
      else
        duplicated_claim = claim.get_duplicated_claim
        if duplicated_claim
          recover_entries(duplicated_claim, claim_data, details, action_on_reversed, carc_on_reversed)
        else
          vlog {"#{prefix} could not find duplicated for claim #{claim.claim_id}"}
        end
      end
      return "reversed"
    end
    return "nothing_to_reverse"
  end

  def self.perform_deductible(entry, details)
    required_keys = %w(contributors)
    optional_keys = %w(ind_deductible fam_deductible carc_on_full carc_on_partial)
    check_action_keys('apply_deductible', details, required_keys, optional_keys)

    claim = entry.get_claim
    insured = claim.insured
    dos = claim.is_claim? ? entry.dos : claim.date_received
    deductible_limit = insured.get_deductible_limits(claim.group_plan_id, dos)
    left_deductible = get_left_deductible(insured, deductible_limit, claim)

    apply_deductible_to_claim_entry(left_deductible, entry, details)
  end

  def self.apply_deductible_to_claim_entry(left_deductible, entry, details)
    amount = [entry.amount_cost.to_f.abs, entry.amount_claim.to_f.abs].min - entry.cob.to_f.abs - entry.copay.to_f.abs
    return if amount.zero? || entry.is_denied?

      if left_deductible >= amount
        entry.deductible = amount
        entry.add_carc(details['carc_on_full']) if details['carc_on_full']
      elsif left_deductible.nonzero?
        entry.deductible = left_deductible
        entry.add_carc(details['carc_on_partial']) if details['carc_on_partial']
      end

    entry.save!
  end

  def self.apply_coinsurance_by_attributes(entry, payable, coinsurance_details, rate_from_attributes)
    apply_coinsurance(entry, payable, coinsurance_details, :rate_from_attributes => rate_from_attributes)
  end

  def self.apply_coinsurance(entry, payable, coinsurance_details, opts= {})
    return if payable <= 0
    coinsurance_rate = opts[:rate_from_attributes] || coinsurance_details['coinsurance_rate']
    entry.coinsurance = entry.get_coinsurance_amount(payable, coinsurance_rate.to_i)
    carc = coinsurance_details['carc_on_result']
    entry.add_carc(carc) if carc && entry.coinsurance > 0
    entry.save!
  end

  def self.find_contributors_entries(entry, condition, opts={})
    opts = {:include_current_entry => false}.merge(opts)
    contributors_entries = []
    filtered_entries = []
    get_binding(:claim_history).each do |claim|
      if claim.document_type == 0
        claim.each_sorted_entry do |e|
          include_current_entry = (e.id.to_i != entry.id.to_i)
          if opts[:include_current_entry]
            include_current_entry = true
          end
          if opts[:exclude_claim].present?
            if e.claim[:id] == opts[:exclude_claim]
              include_current_entry = false
            end
          end
          if !e.is_voided? && (include_current_entry) && (e.status == 1 || e.status == 2)
            if (eval_condition_for_entry(e, condition))
              contributors_entries << e
            end
          end
        end
      end
    end
    if contributors_entries.present?
      contributors_entries.group_by { |e| e.dos }.each do |dos, entries|
        filtered_entries << entries.find_all { |e| e.approved? || e.pended?}
      end
    end
    filtered_entries.compact.flatten
  end

  def self.get_left_deductible(insured, deductible_limits, claim, exclude_current_claim = false)
    return 0.0 if deductible_limits[:ind_deductible] == 0.0 && deductible_limits[:fam_deductible] == 0.0

    used = self.deductible_used(insured, claim, exclude_current_claim)
    if (deductible_limits[:fam_deductible] <= used[:family_used]) ||
        (deductible_limits[:ind_deductible] <= used[:individual_used])
      return 0.0
    end

    left_family_deductible = deductible_limits[:fam_deductible] - used[:family_used]
    left_ind_deductible = deductible_limits[:ind_deductible] - used[:individual_used]

    return left_family_deductible if left_family_deductible < left_ind_deductible
    left_ind_deductible
  end

  def self.deductible_used(insured, claim, exclude_current_claim = false)
    current_deductible_used = 0.0
    deductible_used = {
        :family_used => 0.0,
        :individual_used => 0.0
    }
    unless exclude_current_claim
      claim.claim_entries.each do |ce|
        current_deductible_used +=  ce.deductible.to_f
      end
    end
    deductible_used = insured.deductible_used(claim, {:exclude_current_claim => exclude_current_claim}) if insured
    deductible_used[:individual_used] += current_deductible_used
    deductible_used
  end

  def self.compute_plan_utilization(entry, contributors_entries)
    result = {:benefits_used => 0, :benefits_pending => 0.0, :sum => 0.0}
    use_fee_for_service_in_abm =
      EntityAttribute.find_attr_val_for_entity(entry.get_claim.group_plan,
      entry.get_claim.effective_date, "use_fee_for_service_in_abm_for_fqhc")
    contributors_entries.each do |ce|
      claim_entry_use_fee_for_service = false
      if ce.is_medicaid_reimbursable? && use_fee_for_service_in_abm
        reibursable_fee = ce.get_reimbursable_fee
        reibursable_fee = reibursable_fee.present? ? reibursable_fee.amount.to_f : 0.0
        claim_entry_use_fee_for_service = true
      end
      if ce.claim.paid_eob
        if claim_entry_use_fee_for_service
          result[:benefits_used] += reibursable_fee
        else
          result[:benefits_used] += ce.amount_paid.to_f + ce.tax_withholding.to_f - ce.interest
        end
      else
        if (entry.claim.id != ce.claim_id) || (entry.claim.id == ce.claim.id && ce.id < entry.id)
          if claim_entry_use_fee_for_service
            result[:benefits_pending] += reibursable_fee
          else
            result[:benefits_pending] += ce.get_member_utilization.to_f
          end
        end
      end
    end
    result[:sum] = result[:benefits_used] + result[:benefits_pending]
    result
  end

  def self.compute_plan_family_utilization(condition, opts = {})
    result = {:family_benefits_used => 0.0, :family_benefits_pending => 0.0, :family_sum => 0.0}

    if opts[:family].present?
      family = opts[:family]
      return result unless opts[:plan_id].present?

      plan_id = opts[:plan_id]
    else
      return result unless opts[:entry].present?

      entry = opts[:entry]
      family = entry.claim.insured.family(entry.claim.group_plan_id, entry.dos)
      plan_id = entry.claim.group_plan_id
    end

    family.each do |fam_insured|
      fam_claim = fam_insured.claims.where(:group_plan_id => plan_id)
      if opts[:benefit_period] && opts[:benefit_period].compact.present?
        benefit_period = opts[:benefit_period]
        fam_claim = fam_claim.select do |c|
          (benefit_period[0].to_date..benefit_period[1].to_date).cover?(c.effective_date.to_date)
        end
      end
      fam_claim = fam_claim.first
      next unless fam_claim

      fam_entry = fam_claim.claim_entries.first
      if entry && entry.claim.insured.id == fam_insured.id
        opts[:individual_contributors].each do |ce|
          if ce.claim.paid_eob
            result[:family_benefits_used] += ce.amount_paid.to_f + ce.tax_withholding.to_f - ce.interest
          else
            if entry.claim.id != ce.claim_id || (entry.claim.id == ce.claim.id && ce.id < entry.id)
              result[:family_benefits_pending] += ce.get_member_utilization.to_f
            end
          end
        end
      else
        with_binding(:claim_history, fam_insured && fam_insured.claims,
                     "edn_rule:main", fam_entry) do
          EdnRule.find_contributors_entries(fam_entry, condition, :include_current_entry => true).each do |ce|
            if ce.claim.paid_eob
              result[:family_benefits_used] += ce.amount_paid.to_f + ce.tax_withholding.to_f - ce.interest
            else
              result[:family_benefits_pending] += ce.get_member_utilization.to_f
            end
          end
        end
      end
    end

    result[:family_sum] = result[:family_benefits_used] + result[:family_benefits_pending]
    result
  end

  def self.find_edn_rule(entry, action, context, opts={})
    dos = opts[:dos] ? opts[:dos] : entry.dos
    rules_by_context = EdnRule.rules_for_plan_by_context(
        entry.claim.group_plan,
        {:dos => dos}
    )
    action_context = rules_by_context[context]
    return nil if action_context.blank?

    action_context.detect{|r| r.edn[action].present?}
  end

  def self.find_edn_rules(entry, action, context, opts={})
    dos = opts[:dos] ? opts[:dos] : entry.dos
    rules_by_context = EdnRule.rules_for_plan_by_context(
        entry.claim.group_plan,
        {:dos => dos}
    )
    action_context = rules_by_context[context]
    return nil if action_context.blank?

    action_context.find_all{|r| r.edn[action].present?}
  end

  def self.perform_copay(entry, details)
    rule = get_binding("edn_rule")
    claim_rule_definition = ClaimRuleDefinition.find(rule.instance_variable_get(:@label))
    copay = 0
    if details == "insured_copay"
      insured = entry.get_claim.insured
      insured_copay = insured.get_copay(entry.get_claim.group_plan_id, entry.dos) if insured
      if insured_copay.present? && insured_copay[0].upcase == "Y"
        copay = insured_copay[1]
      end
    else
      copay = details
    end
    rule_cpt_codes = claim_rule_definition.get_cpt_codes
    apply_copay_to_claim_entry(entry, copay, :copay_cpts => rule_cpt_codes)
  end

  def self.perform_recode(entry, details)
    required_keys = ['result','carc_on_original']
    optional_keys = ['carc_on_result','create_result']
    check_action_keys('recode', details, required_keys, optional_keys)

    #vlog {"Starting surface bundling process"}
    create_result = !!(!details.has_key?('create_result') || details['create_result'] == true)

    # Add CARC to entry being processed with the requested CARC
    entry.add_carc(details['carc_on_original'])
    entry.remove_carc(14)
    entry.deny

    # Create result entry
    res_dos = eval_expr(details['result']['dos'])
    if res_dos.is_a?(ClaimEntry) || res_dos.is_a?(MemClaimEntry)
      res_dos = res_dos.dos
    end
    res_cpt = eval_expr(details['result']['cpt'] || details['result']['cdt'])
    res_surfaces = details['result']['surfaces']

    soft_res, hard_res = look_for_result(entry, res_cpt, res_dos)

    if hard_res
      #vlog{"recoding found hard_res: #{res.cpt_code}/#{res.tooth_ids}/#{res.surface_ids}"}
      res = hard_res
    else
      res_fee = nil
      if soft_res
        res = soft_res
        #vlog{"surface_bunding found soft_res: #{res.cpt_code}/#{res.tooth_ids}/#{res.surface_ids}"}
        fsl = res.get_reimbursable_fee
        res_fee = fsl ? fsl.amount.to_f : 0.0
        res.unit_amount_cost = res.unit_amount_benefit = res_fee
        res.save! unless res.claim_id == entry.claim_id
      elsif create_result
        res_surfaces = res_surfaces.present? ? res_surfaces.split(",").join : entry.surface_ids
        res = ClaimEntry.new(
          :status => 2, :created_by => CLAIMS_PROCESSOR_SYSTEM_USER.id, :cpt_code => res_cpt, :dos => res_dos,
          :unit_amount_claim => entry.amount_claim, :claim_id => entry.claim_id,
          :surface_ids => res_surfaces, :tooth_ids => entry.tooth_ids
        )
        fsl = res.get_reimbursable_fee
        res_fee = fsl ? fsl.amount.to_f : 0.0
        if entry.is_a?(ClaimEntry)
          res.unit_amount_cost = res.unit_amount_benefit = res_fee
          res.save! unless res.claim_id == entry.claim_id
        elsif entry.is_a?(MemClaimEntry)
          res_payable = res_fee
          res_billed = entry.amount_claim
          res = MemClaimEntry.new(:claim => entry.claim, :cpt_code => res_cpt, :dos => res_dos, :status => 2,
                                  :unit_amount_claim => res_billed, :amount_claim => res_billed,
                                  :unit_amount_cost => res_payable, :unit_amount_benefit => res_payable,
                                  :amount_cost => res_payable, :amount_benefit => res_payable,
                                  :created_by => CLAIMS_PROCESSOR_SYSTEM_USER.id,
                                  :surface_ids => res_surfaces, :tooth_ids => entry.tooth_ids)
        else
          raise "Unexpected entry type: #{entry.class.name}"
        end
        entry.get_claim.claim_entries << res
      else
        #vlog {"Not bundling - bundle does not exist and bundle creation turned off"}
        return nil
      end

      if details['carc_on_result']
        res.add_carc(details['carc_on_result'])
        res.save! unless res.claim_id == entry.claim_id
      end
    end
    nil
  end

  def self.perform_allowed(prefix, action, entry, details)
    required_keys = ['apply_carc','amount']
    optional_keys = []
    check_action_keys(action, details, required_keys, optional_keys)

    fsl = entry.get_reimbursable_fee
    fee_schedule_allowed_amount = fsl ? fsl.amount.to_f : 0.0
    vlog {"#{prefix} The fee schedule amount for #{entry.cpt_code} is #{fee_schedule_allowed_amount} in claim #{entry.claim.claim_id}"}
    allowed_amount_adjustment = details['amount'].to_f

    if action ==  'allowed_exactly'
      vlog {"#{prefix} Setting amount allowed exactly to #{allowed_amount_adjustment} in claim #{entry.claim.claim_id}"}
      entry.unit_amount_cost = entry.unit_amount_benefit = allowed_amount_adjustment
      Rdf.create!(:subject => entry, :p => 'amount-allowed-exactly-to', :system_user_sid => 1, :oflt => allowed_amount_adjustment,
        :ostr => "#{prefix} Setting amount allowed exactly to #{allowed_amount_adjustment}", :created_at_date => Time.now,
        :last_updated_date => Time.now, :provenance =>  __method__.to_s)
    elsif action ==  'allowed_increase_to'
      if fee_schedule_allowed_amount < allowed_amount_adjustment
        vlog {"#{prefix} Setting amount allowed up to #{allowed_amount_adjustment} in claim #{entry.claim.claim_id}"}
        entry.unit_amount_cost = entry.unit_amount_benefit = allowed_amount_adjustment
        Rdf.create!(:subject => entry, :p => 'amount-allowed-increased-to', :system_user_sid => 1, :oflt => allowed_amount_adjustment,
          :ostr => "#{prefix} Setting amount allowed increased to #{allowed_amount_adjustment}", :created_at_date => Time.now,
          :last_updated_date => Time.now, :provenance =>  __method__.to_s)
      end
    elsif action ==  'allowed_increase_by'
      vlog {"#{prefix} Setting amount allowed increased by #{allowed_amount_adjustment} in claim #{entry.claim.claim_id}"}
      entry.unit_amount_cost = entry.unit_amount_benefit = fee_schedule_allowed_amount + allowed_amount_adjustment
      Rdf.create!(:subject => entry, :p => 'amount-allowed-increase-by', :system_user_sid => 1, :oflt => allowed_amount_adjustment,
        :ostr => "#{prefix} Setting amount allowed increased by #{allowed_amount_adjustment}", :created_at_date => Time.now,
        :last_updated_date => Time.now, :provenance =>  __method__.to_s)
    else
      rule_error "#{prefix}unsupported action #{action.to_edn} with details #{details.to_edn}"
    end
    add_carcs_to_entry(entry, details['apply_carc'])
  end

  def self.perform_deny_higher_fees(entry, details)
    required_keys = ['contributors', 'carc_on_over_max','max_frequency']
    optional_keys = []
    check_action_keys('deny_higher_fees', details, required_keys, optional_keys)

    contributors_entries = find_contributors_entries(entry, details["contributors"], :include_current_entry => true)
    same_dos_entries = contributors_entries.select{|ce| ce.dos == entry.dos}
    same_dos_entries_fees = same_dos_entries.collect{|ce| [ce, ce.get_reimbursable_fee ? ce.get_reimbursable_fee.amount : 0]}

    max_frequency = details["max_frequency"].to_i
    if contributors_entries.size > max_frequency
      same_dos_entries_fees_sorted = same_dos_entries_fees.sort_by{|ce| ce[1]}.reverse
      number_of_entries_to_deny = contributors_entries.size - max_frequency

      # Entries to deny are the ones with the higher payable fee
      entries_to_deny = same_dos_entries_fees_sorted.first(number_of_entries_to_deny)
      entries_to_deny.each do |ce|
        ce[0].add_carc(details["carc_on_over_max"])
        ce[0].deny
      end
    end
  end

  def self.perform_coinsurance(entry, details)
    required_keys = %w(coinsurance_rate)
    optional_keys = %w(carc_on_result)
    check_action_keys('apply_coinsurance', details, required_keys, optional_keys)
    rule_cpt_codes = []
    rule = get_binding("edn_rule")

    claim_rule_definition = ClaimRuleDefinition.find_by_id(rule.instance_variable_get(:@label))
    if details['coinsurance_rate'].is_a?(Integer)
      rule_cpt_codes = claim_rule_definition.get_cpt_codes
    else
      attribute_with_rates = details['coinsurance_rate']
      group_rider = entry.group_rider! rescue nil
      group_rider_details = JSON.parse(group_rider.details) rescue nil
      if group_rider_details && group_rider_details.key?(attribute_with_rates)
        rule_cpt_codes = claim_rule_definition.get_cpt_codes
        details['coinsurance_rate'] = group_rider_details[attribute_with_rates]
      else
        claim = entry.get_claim
        insured = claim.insured
        rule_cpt_codes = claim_rule_definition.try(:get_cpt_codes)
        rate_from_attributes = insured.get_rate_from_entity_attributes(claim.group_plan, entry.consider_date, attribute_with_rates)
      end
    end

    return nil if rule_cpt_codes.blank? || !is_cpt_in_conditions?(entry.cpt_code, rule_cpt_codes)
    apply_coinsurance_to_claim_entry(entry, rate_from_attributes, details)
  end

  def self.apply_coinsurance_to_claim_entry(entry, rate_from_attributes, details)
    amount = [entry.amount_cost, entry.amount_claim].min + entry.interest - entry.cob
    payable = amount - entry.copay - entry.deductible
    if rate_from_attributes.present?
      apply_coinsurance_by_attributes(entry, payable, details, rate_from_attributes)
    elsif details.present? && (Integer(details['coinsurance_rate']) rescue nil)
      apply_coinsurance(entry, payable, details)
    end
  end

=begin
Rule F0002

mdh = entry.rendered_by_mdh? == :provider
doses = claim.is_claim? ? claim.claim_entries.collect {|ce| ce.dos}.compact.uniq : [claim.date_received]
doses.each do |ddos|
  contracted = billed = paid = 0
  xrays = ClaimEntry.rule_find(:insured => insured.id,
  :facility => mdh ? nil : facility.id,
  :status => [1,2],
  :on_dos => ddos,
  :cdts => %w(0210 0220 0230 0250 0270 0272 0273 0274 0330))
  xrays.each do |e|
    next if e.is_denied? || e.has_carc?(18)
    fee=e.get_reimbursable_fee
    contracted += (fee.amount.to_f * e.qty).abs * (e.reversed? ? -1 : 1) if fee
    billed += e.amount_claim.to_f
    paid += e.amount_paid.to_f
  end
  fee = entry.get_reimbursable_fee('0210')
  ce0210 = nil
  if fee && fee.amount.to_f > 0.0 && contracted > 0.0 && contracted > fee.amount.to_f
    xrays.each do |e|
      next if e.amount_paid.to_f > 0 || e.claim.paid_eob || e.reversed? || e.is_denied?
      if e.cpt_code.to_s == '0210'
        ce210 = e.id
        next
      end
      e.add_carc(24, true)
      e.deny
      e.remove_carc(14)
      e.save
    end

    if ClaimEntry.rule_find(:insured => insured.id,
      :facility => mdh ? nil : facility.id,
      :status => [0,1,2,3],
      :on_dos => ddos,
      :cdts => '0210').size == 0
      logger.error("CREATING 0210")
      new_entry = dup_entry(entry, '0210')
      new_entry.tooth_ids = nil
      new_entry.surface_ids = nil
      new_entry.area_ids = nil
      new_entry.unit_amount_claim = (billed - paid)
      new_entry.unit_amount_cost = (fee.amount.to_f rescue 0) - paid
      new_entry.unit_amount_benefit = (fee.amount.to_f rescue 0) - paid
      new_entry.created_by = 855
      new_entry.status = 0
      new_entry.save

      if new_entry.amount_cost < new_entry.amount_claim
        new_entry.add_carc(14)
      end
      claim_entries << new_entry
      logger.error('added')
    end
  end
end

=end

  def self.perform_edn_action(entry, action, details)
    if (details.is_a?(Enumerable) and not details.is_a?(String)) and
        ['deny','strong_deny','approve_at_zero','pend', 'strong_pend'].include? action
      vlog {"Distributing #{action} across #{details.inspect}"}
      details.each do |detail|
        perform_edn_action entry, action, detail
      end
      return
    end
    rule = get_binding("edn_rule")
    rule_text = rule ? "Rule #{rule.label}: " : ''
    rule_description = rule ? "#{rule.description}" : ''
    vlog{"Applying #{rule_text} #{rule_description}"}
    vlog{"#{rule_text} #{rule.edn.to_edn}"}
    prefix = rule_text + entry.to_edn + " "
    if ['deny','strong_deny', 'footer_deny'].include?(action) && details.is_a?(Integer)
      if entry.exempt_from(details)
        vlog{"#{prefix} would have denied with carc #{details} but entry is exempt"}
      else
        vlog{"#{prefix} #{action}ing with carc #{details}"}
        entry.add_carc(details)
        entry.deny
        action_type = (action == 'footer_deny') ? :footer_deny : :deny
        get_binding(:adjudication_actions) << { :entry => entry, :action => action_type,
                                                :strongly => (action == 'strong_deny') }
      end
    elsif action == 'approve_at_zero' && details.is_a?(Integer)
      vlog{"#{prefix}approving at $0 with CARC #{details}"}
      entry.unit_amount_cost = entry.unit_amount_benefit = entry.amount_cost = entry.amount_benefit = 0.0
      entry.add_carc(details)
      entry.approve
      get_binding(:adjudication_actions) << { :entry => entry, :action => :approve_at_zero }
    elsif action == 'adjust_by' && details.is_a?(Hash)
      details.keys.each do |key|
        unless ["apply_carc", "amount"].include? key
          rule_error "Invalid key(s) in details for #{action}: #{ details.keys } must have amount, may have apply_carc, nothing else is allowed"
        end
      end
      apply_carc = (details["apply_carc"] || 18).to_i
      fee_adjustment = details["amount"].to_f
      vlog{"#{prefix}adjusting by #{fee_adjustment} with CARC #{apply_carc}"}
      Rdf.create!(:subject => entry, :p =>'use-fee-adjust-by', :system_user_sid => 1, :oflt => fee_adjustment,
      :ostr => "#{prefix} Adjusting to #{fee_adjustment} with CARC #{apply_carc}", :created_at_date => Time.now,
      :last_updated_date => Time.now, :provenance =>  __method__.to_s)
      entry.unit_amount_cost = entry.unit_amount_benefit = entry.amount_cost = entry.amount_benefit =
        entry.get_reimbursable_fee.amount.to_f
      entry.add_carc(apply_carc)
      entry.approve
      get_binding(:adjudication_actions) << { :entry => entry, :action => :adjust }
    elsif (action == 'allowed_exactly' || action == 'allowed_increase_to' || action == 'allowed_increase_by') && details.is_a?(Hash)
      vlog{"#{prefix}#{action}: #{details.to_edn}"}
      if entry.get_claim.is_claim?
        perform_allowed(prefix, action, entry, details)
      end
    elsif %w{strong_deny deny pend strong_pend approve_at_zero}.include?(action) && details.is_a?(String)
      vlog{"#{prefix}would #{action}, but this carc is invalid: #{details}"}
    elsif (action == 'pend' || action == 'strong_pend' || action == 'footer_pend') && details.is_a?(Integer)
      vlog{"#{prefix} #{action} with carc #{details}"}
      entry.add_carc(details, false)
      entry.pend
      action = action == 'footer_pend' ? :footer_pend : :pend
      get_binding(:adjudication_actions) << { :entry => entry, :action => action,
                                              :strongly => (action == 'strong_pend') }
    elsif action == 'bundle'
      vlog{"#{prefix}bundle: #{details.to_edn}"}
      is_medicaid_reimbursable = false
      if (entry.get_claim.document_type == 0 && entry.get_claim.provider_facility &&
          entry.medicaid_reimbursement_rate)
        is_medicaid_reimbursable = true
      end

      if entry.get_claim.is_claim?
        # Perform bundling if facility is not medicaid reimbursable (FQHC/IHC) so that only the FQHC/IHC entry pays
        if !is_medicaid_reimbursable
          if entry.exempt_from(details['carc_on_bundlables'])
            vlog{"#{prefix}would have bundled but entry is exempt"}
          else
            perform_bundling(entry, details)
            get_binding(:adjudication_actions) << { :entry => entry, :action => :bundle }
          end
        else
          vlog{"Skipping bundling for medicaid reimbursable (FQHC IHC)"}
        end
      else
        vlog{"Skipping bundling for non-claim"}
      end
    elsif action == 'bundle_surfaces'
      vlog{"#{prefix}bundle_surfaces: #{details.to_edn}"}
      if entry.get_claim.is_claim?
        if entry.exempt_from(details['carc_on_bundlables'])
            vlog{"#{prefix}would have bundled but entry is exempt"}
        else
          perform_surface_bundling(entry, details)
          get_binding(:adjudication_actions) << { :entry => entry, :action => :bundle_surfaces }
        end
      else
        vlog{"Skipping surface bundling for non-claim"}
      end
    elsif action == 'limit_total'
      vlog{"#{prefix}#{action}: #{details.to_edn}"}
      if entry.get_claim.is_claim?
        perform_limit_total(action, entry, details)
      end
    elsif action == 'recoup'
      vlog{"#{prefix}#{action}: #{details.to_edn}"}
      if entry.get_claim.is_claim?
        perform_recoup(action, entry, details)
      end
    elsif action == 'reverse'
      vlog{"#{prefix}#{action}: #{details.to_edn}"}
      if entry.get_claim.is_claim?
        action_taken = perform_reverse(action, entry, details)
        if (action_taken == "not_reversed")
          vlog{"Not able to take action #{prefix}#{action}, is already taken"}
        elsif (action_taken == "reversed")
          vlog{"Action #{prefix}#{action} has been applied"}
        end
      end
    elsif action == 'add_copay'
      vlog{"#{prefix}add_copay: #{details.to_edn}"}
      claim = entry.get_claim
      if claim.is_claim? || claim.is_preauth?
        perform_copay(entry, details)
      end
    elsif action == 'recode'
      vlog{"#{prefix}recode: #{details.to_edn}"}
      perform_recode(entry, details)
    elsif action == 'deny_higher_fees'
      vlog{"#{prefix}deny_higher_fees: #{details.to_edn}"}
      perform_deny_higher_fees(entry, details)
    elsif action == 'apply_deductible'
      vlog{"#{prefix}#{action}: #{details.to_edn}"}
      claim = entry.get_claim
      if claim.is_claim? || claim.is_preauth?
        perform_deductible(entry, details)
      end
    elsif action == 'apply_coinsurance'
      vlog{"#{prefix}#{action}: #{details.to_edn}"}
      claim = entry.get_claim
      if claim.is_claim? || claim.is_preauth?
        perform_coinsurance(entry, details)
      end
    elsif action == 'limit_lifetime_total'
      vlog{"#{prefix}#{action}: #{details.to_edn}"}
      claim = entry.get_claim
      if claim.is_claim?
        perform_limit_lifetime_total(action, entry, details)
      end
    elsif action == 'apply_max_out_of_pocket'
      vlog{"#{prefix}#{action}: #{details.to_edn}"}
      claim = entry.get_claim
      if claim.is_claim?
        perform_apply_moop(entry, details)
      end
    elsif action == 'add_carc'
      if details.is_a?(Integer)
        vlog { "#{prefix} #{action} with carc #{details}" }
        entry.add_carc(details)
        entry.save!
      elsif details.is_a?(String)
        vlog { "#{prefix}would #{action}, but this carc is invalid: #{details}" }
      end
    else
      rule_error "#{prefix}unsupported action #{action.to_edn} with details #{details.to_edn}"
    end
  end

  def self.fill_missing_period_points(benefit_period, dos)
    [benefit_period[0].presence || "#{dos.year}-01-01".to_date,
     benefit_period[1].presence || "#{dos.year}-12-31".to_date]
  end

  def self.group_riders_benefit_period(gpi, group, dos)
    return nil if gpi.blank? || group.blank?

    subscriber_attribute = SubscriberAttribute.
      where(
        :subscriber_id => gpi.subscriber_id,
        :group_plan_id =>  gpi.group_plan_id
      ).
      where("? >= effective_date and ? <= coalesce(termination_date, 'infinity')", dos, dos).
      first
    return nil if subscriber_attribute.blank?

    begin
      subscriber_attribute = JSON.parse(subscriber_attribute.attribs)
      group_rider = GroupRider.
        where(
          :group_id => group.id,
          :benefit_level_indicator => subscriber_attribute['benefit-package']
        ).
        where("? >= group_riders.effective_date and ? <= coalesce(group_riders.termination_date, 'infinity')", dos, dos).
        first
      if group_rider.present?
        return fill_missing_period_points([group_rider.effective_date, group_rider.termination_date], dos)
      end
    rescue JSON::ParserError => e
      vlog { e.message }
      return nil
    end
  end

  def self.attributes_benefit_period(entity, dos)
    return nil if entity.blank?

    attributes = EntityAttribute.find_attributes_for_entity(entity, dos)
    if attributes.present? && attributes.benefit_thru.present? && attributes.benefit_from.present?
      return Adhoc::compute_benefit_periods(dos, attributes.benefit_from, attributes.benefit_thru).map(&:to_date)
    end
  end

  def self.identify_benefit_period(entry)
    claim = entry.claim
    insured = claim.insured
    dos = entry.dos
    group_plan_id = claim.group_plan_id
    gpi = GroupPlanInsured.where(:group_plan_id => group_plan_id, :insured_id => insured.id).max_by(&:effective_date)
    group = Group.find_by_id(gpi.try(:group_id))

    group_riders_benefit_period(gpi, group, dos)
  end

  def self.individual_max_oop(entry, benefit_period)
    sql = %Q{
    select pro.individual_value
    from patient_responsibility_overrides pro
    join group_plans_insureds gpi on gpi.subscriber_id = pro.subscriber_id and pro.group_id = gpi.group_id
    join claims c on c.insured_id = gpi.insured_id and
                     c.insured_ssn = gpi.subscriber_id and
                     c.group_plan_id = gpi.group_plan_id
    where c.id = #{entry.claim_id} and
      #{sql_escape(entry.dos)} >= gpi.effective_date and
      #{sql_escape(entry.dos)} <= coalesce(gpi.termination_date, 'infinity') and
      pro.effective_date >= #{sql_escape(benefit_period[0].to_date)} and
      pro.effective_date <= #{sql_escape(benefit_period[1].to_date)} and
      pro.component = 'oop_max'
    order by pro.effective_date desc, pro.created_at desc
    limit 1
    }
    db_select(sql).first.try(:with_indifferent_access).try(:individual_value).to_f
  end

  def self.individual_current_oop(entry, benefit_period)
    sql = %Q{
    select pro.individual_value, pro.effective_date
    from patient_responsibility_overrides pro
    join group_plans_insureds gpi on gpi.subscriber_id = pro.subscriber_id and pro.group_id = gpi.group_id
    join claims c on c.insured_id = gpi.insured_id and
                     c.insured_ssn = gpi.subscriber_id and
                     c.group_plan_id = gpi.group_plan_id
    where c.id = #{entry.claim_id} and
      #{sql_escape(entry.dos)} >= gpi.effective_date and
      #{sql_escape(entry.dos)} <= coalesce(gpi.termination_date, 'infinity') and
      pro.effective_date >= #{sql_escape(benefit_period[0].to_date)} and
      pro.effective_date <= #{sql_escape(benefit_period[1].to_date)} and
      pro.component = 'oop_so_far'
    order by pro.effective_date desc, pro.created_at desc
    limit 1
    }
    db_select(sql).first.try(:with_indifferent_access)
  end

  def self.relevant_entries(entry, effective_date, benefit_period, contributors, from_oop_so_far)
    condition = contributors.presence || true
    insured = entry.claim.insured
    entries = []
    relevant_claims = insured.claims.select do |claim|
      claim.group_plan_id == entry.claim.group_plan_id
    end

    # we collect entries that were either paid after the effective_date
    # or processed bot not yet paid
    with_binding(:claim_history, relevant_claims.each{ |claim| claim.claim_entries.reload }) do
      EdnRule.find_contributors_entries(entry, condition).each do |contributor_entry|
        condition_for_paid =
          if from_oop_so_far
            contributor_entry.claim.paid_eob && contributor_entry.claim.paid_eob > effective_date
          else
            contributor_entry.claim.paid_eob && contributor_entry.claim.paid_eob >= effective_date
          end
        if (benefit_period[0].to_s..benefit_period[1].to_s).cover?(contributor_entry.dos.to_s) && (!contributor_entry.claim.paid_eob || condition_for_paid)
          entries << contributor_entry
        end
      end
    end

    entries.reject { |contributor_entry| contributor_entry.id > entry.id && contributor_entry.claim_id == entry.claim_id }
  end

  def self.relevant_family_entries(entry, effective_date, benefit_period, contributors, from_oop_so_far)
    entries = []
    insured = entry.claim.insured
    group_plan = entry.claim.group_plan
    condition = contributors.presence || true
    family_claims = insured.family(group_plan.id, Date.today).
                            map{ |member| member.claims.each{ |claim| claim.claim_entries.reload } }.
                            flatten.
                            select do |claim|
                              claim.group_plan_id == entry.claim.group_plan_id
                            end

    # we collect entries that were either paid after the effective_date
    # or processed bot not yet paid
    with_binding(:claim_history, family_claims) do
      EdnRule.find_contributors_entries(entry, condition).each do |contributor_entry|
        condition_for_paid =
          if from_oop_so_far
            contributor_entry.claim.paid_eob && contributor_entry.claim.paid_eob > effective_date
          else
            contributor_entry.claim.paid_eob && contributor_entry.claim.paid_eob >= effective_date
          end
        if (benefit_period[0].to_s..benefit_period[1].to_s).cover?(contributor_entry.dos.to_s) && (!contributor_entry.claim.paid_eob || condition_for_paid)
          entries << contributor_entry
        end
      end
    end

    entries.select do |contributor_entry|
      contributor_entry.claim_id != entry.claim_id || contributor_entry.id <= entry.id
    end
  end

  def self.reduce_entry_contributors(entry, amount_exceeding_moop)
    new_attributes = {}
    return new_attributes if amount_exceeding_moop.blank?

    if amount_exceeding_moop > entry.coinsurance.to_f + entry.deductible.to_f + entry.copay.to_f
      new_attributes = {
        :coinsurance => 0,
        :copay => 0,
        :deductible => 0
      }
    else
      if entry.coinsurance >= amount_exceeding_moop
        new_attributes[:coinsurance] = entry.coinsurance - amount_exceeding_moop
      else
        new_attributes[:coinsurance] = 0
        if entry.coinsurance + entry.deductible >= amount_exceeding_moop
          new_attributes[:deductible] = entry.deductible - (amount_exceeding_moop - entry.coinsurance)
        else
          new_attributes[:deductible] = 0
          new_attributes[:copay] = entry.copay - (amount_exceeding_moop - entry.coinsurance - entry.deductible)
        end
      end
    end

    new_attributes
  end

  def self.individual_estimate_oop(entry, benefit_period, contributors)
    current_oop_entry = individual_current_oop(entry, benefit_period)
    if current_oop_entry.blank?
      current_oop = 0
      relevant_entries = relevant_entries(entry, benefit_period[0].to_date, benefit_period, contributors, false)
    else
      current_oop = current_oop_entry[:individual_value].to_f
      relevant_entries = relevant_entries(entry, current_oop_entry[:effective_date].to_date, benefit_period, contributors, true)
    end
    relevant_entries = relevant_entries + [entry] if eval_condition_for_entry(entry, contributors)
    total_patient_responsibility = relevant_entries.
      sum { |relevant_entry| relevant_entry.coinsurance.to_f + relevant_entry.deductible.to_f + relevant_entry.copay.to_f }

    total_patient_responsibility + current_oop
  end

  def self.individual_moop_excess(entry, ind_max_out_of_pocket, contributors, benefit_period)
    return nil if ind_max_out_of_pocket.blank? || ind_max_out_of_pocket == 0

    maximum_oop =
      if ind_max_out_of_pocket == 'oop_max'
        self.individual_max_oop(entry, benefit_period)
      else
        ind_max_out_of_pocket
      end

    return nil if maximum_oop == 0.0
    estimate_oop = self.individual_estimate_oop(entry, benefit_period, contributors)
    return nil if estimate_oop <= maximum_oop

    estimate_oop - maximum_oop
  end

  def self.family_max_oop(entry, benefit_period)
    sql = %Q{
    select pro.family_value
    from patient_responsibility_overrides pro
    join group_plans_insureds gpi on gpi.subscriber_id = pro.subscriber_id and pro.group_id = gpi.group_id
    join claims c on c.insured_id = gpi.insured_id and
                     c.insured_ssn = gpi.subscriber_id and
                     c.group_plan_id = gpi.group_plan_id
    where c.id = #{entry.claim_id} and
      #{sql_escape(entry.dos)} >= gpi.effective_date and
      #{sql_escape(entry.dos)} <= coalesce(gpi.termination_date, 'infinity') and
      pro.effective_date >= #{sql_escape(benefit_period[0].to_date)} and
      pro.effective_date <= #{sql_escape(benefit_period[1].to_date)} and
      pro.component = 'oop_max'
    order by pro.effective_date desc, pro.created_at desc
    limit 1
    }
    db_select(sql).first.try(:with_indifferent_access).try(:family_value).to_f
  end

  def self.family_current_oop(entry, benefit_period)
    sql = %Q{
    select pro.family_value, pro.effective_date
    from patient_responsibility_overrides pro
    join group_plans_insureds gpi on gpi.subscriber_id = pro.subscriber_id and pro.group_id = gpi.group_id
    join claims c on c.insured_id = gpi.insured_id and
                     c.insured_ssn = gpi.subscriber_id and
                     c.group_plan_id = gpi.group_plan_id
    where c.id = #{entry.claim_id} and
      #{sql_escape(entry.dos)} >= gpi.effective_date and
      #{sql_escape(entry.dos)} <= coalesce(gpi.termination_date, 'infinity') and
      pro.effective_date >= #{sql_escape(benefit_period[0].to_date)} and
      pro.effective_date <= #{sql_escape(benefit_period[1].to_date)} and
      pro.component = 'oop_so_far'
    order by pro.effective_date desc, pro.created_at desc
    limit 1
    }
    db_select(sql).first.try(:with_indifferent_access)
  end

  def self.estimate_family_oop(entry, benefit_period, contributors)
    current_oop_entry = family_current_oop(entry, benefit_period)
    if current_oop_entry.blank?
      current_oop = 0
      relevant_entries = relevant_family_entries(entry, benefit_period[0].to_date, benefit_period, contributors, false)
    else
      current_oop = current_oop_entry[:family_value].to_f
      relevant_entries = relevant_family_entries(entry, current_oop_entry[:effective_date].to_date, benefit_period, contributors, true)
    end
    relevant_entries = relevant_entries + [entry] if eval_condition_for_entry(entry, contributors)
    total_patient_responsibility = relevant_entries.
      sum { |relevant_entry| relevant_entry.coinsurance.to_f + relevant_entry.deductible.to_f + relevant_entry.copay.to_f }

    total_patient_responsibility + current_oop
  end

  def self.family_moop_excess(entry, fam_max_out_of_pocket, contributors, benefit_period)
    return nil if fam_max_out_of_pocket.blank? || fam_max_out_of_pocket == 0

    maximum_oop =
      if fam_max_out_of_pocket == 'oop_max'
        self.family_max_oop(entry, benefit_period)
      else
        fam_max_out_of_pocket
      end

    return nil if maximum_oop == 0.0
    estimate_oop = self.estimate_family_oop(entry, benefit_period, contributors)
    return nil if estimate_oop <= maximum_oop

    estimate_oop - maximum_oop
  end

  def self.perform_apply_moop(entry, details)
    benefit_period = self.identify_benefit_period(entry)
    return nil if benefit_period.blank?

    individual_moop_excess = self.individual_moop_excess(entry, details['ind_max_out_of_pocket'], details['contributors'], benefit_period)
    family_moop_excess = self.family_moop_excess(entry, details['fam_max_out_of_pocket'], details['contributors'], benefit_period)
    resulting_excess = [individual_moop_excess, family_moop_excess].compact.max
    entry.tap do |e|
      reductions = reduce_entry_contributors(entry, resulting_excess)
      e.assign_attributes(reductions)
      e.save!
    end
  end
=begin
  ce = ClaimEntry.find(51139834)

  age_rule =
    {"when" => ["and", ["=", "cpt", "0120,0150,0330"],
                       ["or", ["<", "age", 3],
                              [">", "age", 16]]],
     "deny" => 62}

  EdnRule.new(age_rule).eval_rule_for_entry(ce) # deny 62 because insured is over age 16

=end

  def eval_rule_for_entry(entry, opts={})
    with_binding("edn_rule", self,
                 "edn_rule:main", entry,
                 "edn_rules:found_entries",{}) {
      action_keys = @edn.keys - ["when"]
      EdnRule.rule_error "Rule must specify actions" if action_keys.empty?

      tag_exceptions_with("<rule:#{get_binding('edn_rule') ? get_binding('edn_rule').label : "none"}>") {
        condition_met =
          if @edn.has_key?("when")
            EdnRule.eval_condition_for_entry(entry, @edn["when"])
          else
            true
          end
        if condition_met
          action_keys.each {|k| EdnRule.perform_edn_action(entry, k, @edn[k])}
        end
        EdnRule.on_claim_rule_execution_completed()
      }
    }
  end

  def self.context_order
    %w(strong_deny strong_pend dup_deny strong_approve deny approve_at_zero adjust loi_deny pend fees bundle discount
       apply_deductible apply_coinsurance apply_max_out_of_pocket limit_total footer_deny footer_pend footer_add_carc)
  end

  def self.action_context(action,details)
    case action
    when 'strong_deny', 'strong_pend', 'discount', 'pend', 'bundle', 'apply_coinsurance', 'apply_deductible'
      action
    when 'deny'
      if (IneligibleCode.untimely_filing_ids + IneligibleCode.expired_loi_ids).include?(details)
        'strong_deny'
      elsif details == 2
        'dup_deny'
      elsif IneligibleCode.loi_ids.include?(details)
        'loi_deny'
      else
        'deny'
      end
    when 'approve_at_zero'
      if details == 7
        'strong_approve'
      else
        'approve_at_zero'
      end
    when 'recoup', 'reverse', 'adjust_by'
      'adjust'
    when 'allowed_exactly', 'allowed_increase_to', 'allowed_increase_by'
      'discount'
    when 'bundle_surfaces', 'recode', 'deny_higher_fees'
      'bundle'
    when 'add_copay'
      'discount'
    when 'limit_total', 'limit_lifetime_total'
      'limit_total'
    when 'apply_max_out_of_pocket'
      'apply_max_out_of_pocket'
    when 'footer_deny'
      'footer_deny'
    when 'footer_pend'
      'footer_pend'
    when 'add_carc'
      'footer_add_carc'
    else
      vlog {"Unable to determine context for action #{action} with details #{details.to_edn}"}
      nil
    end
  end

  def applicable_contexts
    (@edn.keys - ["when"]).map{|action| EdnRule.action_context(action, @edn[action])}.compact.uniq
  end

  def self.assert_entry(entry, opts={})
    unless entry.is_a?(ClaimEntry) || entry.is_a?(MemClaimEntry)
      rule_error("arg must be claim entry, found #{entry.class.name}" +
                 opts.keys.map{|k| "#{k}=#{opts[k]}"}.join(", "))
    end
  end

  def self.rule_error(error)
    rule = get_binding('edn_rule')
    entry = get_binding('edn_rule:main')
    raise "[Rule: #{rule ? rule.label : 'no rule'} | Entry: #{entry ? entry.id : 'no entry'}] \n #{error}"
  end
end
