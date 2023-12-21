import apache_beam as beam

p1 = beam.Pipeline()

attendance_count = (
    p1
    | beam.io.ReadFromText('data/dept_data.txt')
    | beam.Map(lambda record: record.split(','))
    | beam.Filter(lambda record: record[3] == 'Accounts')
    | beam.Map(lambda record: (record[1], 1))
    | beam.CombinePerKey(sum)
    | beam.io.WriteToText('data/output_new_final')
)

p1.run()