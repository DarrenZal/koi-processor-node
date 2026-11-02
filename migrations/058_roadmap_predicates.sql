-- Migration 058: Roadmap entity predicates
-- Adds predicates for roadmap graph relationships (Outcome, Initiative,
-- WorkItem, Milestone, Decision, Risk, Metric).

INSERT INTO allowed_predicates (predicate, description, subject_types, object_types) VALUES
  ('depends_on', 'Node depends on another node',
   ARRAY['Outcome','Initiative','WorkItem','Milestone'],
   ARRAY['Outcome','Initiative','WorkItem','Milestone']),
  ('delivers', 'Work item/initiative delivers to outcome/milestone',
   ARRAY['WorkItem','Initiative'],
   ARRAY['Outcome','Milestone']),
  ('measures', 'Metric measures an outcome',
   ARRAY['Metric'],
   ARRAY['Outcome']),
  ('mitigates', 'Work mitigates a risk',
   ARRAY['WorkItem','Initiative','Decision'],
   ARRAY['Risk']),
  ('blocks', 'Node blocks another node',
   ARRAY['Risk','WorkItem','Initiative','Decision'],
   ARRAY['WorkItem','Initiative','Milestone']),
  ('references', 'General reference link',
   ARRAY['Outcome','Initiative','WorkItem','Decision','Risk','Milestone','Metric'],
   ARRAY['Outcome','Initiative','WorkItem','Decision','Risk','Milestone','Metric'])
ON CONFLICT (predicate) DO NOTHING;
