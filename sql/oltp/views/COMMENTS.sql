-- COMMENTS -------------------------------------------------------------------

COMMENT ON TABLE CORE.CORE_GRGR_GROUP IS 'Employer Group - Organization/group sponsoring healthcare coverage.';
COMMENT ON TABLE CORE.CORE_PLPL_PLAN IS 'Benefit Plan - Defines the healthcare benefit/coverage plan.';
COMMENT ON TABLE CORE.CORE_MESB_SUBSCRIBER IS 'Subscriber - Primary enrollee/policyholder associated with an employer group.';
COMMENT ON TABLE CORE.CORE_MEME_MEMBER IS 'Member - Covered individual. May represent subscriber or dependent.';
COMMENT ON TABLE CORE.CORE_SBAD_ADDRESS IS 'Member address information.';
COMMENT ON TABLE CORE.CORE_MEEN_ENROLLMENT IS 'Enrollment - Represents a member''s coverage relationship with a benefit plan.';
COMMENT ON TABLE ENRL.ENRL_EDXN_TRANSACTION IS '834 Transaction Control - Stores inbound transaction envelope information, processing status and source file metadata.';
COMMENT ON TABLE ENRL.ENRL_RJRW_REJECT IS 'Rejected enrollment record - Stores invalid records that fail validation or business rules.';
