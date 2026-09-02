# Define bucket name
BUCKET="med834enrl-s3"

# Remove all folder placeholders

# Landing
aws s3api delete-object --bucket $BUCKET --key "landing/834/"

# Archive
aws s3api delete-object --bucket $BUCKET --key "archive/834/"

# Lakehouse - Bronze
aws s3api delete-object --bucket $BUCKET --key "lakehouse/bronze/edi_834_raw/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/bronze/edi_834_segments/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/bronze/ingestion_metadata/"

# Lakehouse - Silver
aws s3api delete-object --bucket $BUCKET --key "lakehouse/silver/member/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/silver/subscriber/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/silver/employer_group/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/silver/benefit_plan/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/silver/enrollment/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/silver/address/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/silver/transaction/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/silver/rejected_record/"

# Lakehouse - Gold
aws s3api delete-object --bucket $BUCKET --key "lakehouse/gold/dim_member/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/gold/dim_plan/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/gold/dim_group/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/gold/dim_date/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/gold/dim_transaction_source/"
aws s3api delete-object --bucket $BUCKET --key "lakehouse/gold/fact_enrollment/"

# Quarantine
aws s3api delete-object --bucket $BUCKET --key "quarantine/834/invalid_format/"
aws s3api delete-object --bucket $BUCKET --key "quarantine/834/validation_failed/"
aws s3api delete-object --bucket $BUCKET --key "quarantine/834/processing_failed/"

# Audit
aws s3api delete-object --bucket $BUCKET --key "audit/ingestion/"
aws s3api delete-object --bucket $BUCKET --key "audit/processing/"
aws s3api delete-object --bucket $BUCKET --key "audit/reconciliation/"
aws s3api delete-object --bucket $BUCKET --key "audit/errors/"

echo "All folder placeholders removed from $BUCKET"
# WARNING: This deletes ALL objects including real data

aws s3 rm s3://$BUCKET --recursive

echo "All objects removed from $BUCKET"
