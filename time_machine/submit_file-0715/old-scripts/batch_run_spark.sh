topic_size=100
sampling_rate=0.000127
kappa=1.02
tau0=64
max_iter=400
worker_num=16
optimizer="online_lr"
submit_file="define_lda_submit_pubmed.sh"

#./pubmed_balance_dataset.sh $worker_num

for tau_init in 100 10 1000
do
	for sample_fraction in 0.000505 0.00202
	do
		for k in 1.02 1.0 1.04 1.08
		do
			./$submit_file $topic_size $sample_fraction $k $tau_init $max_iter $worker_num $optimizer
			./assemble_logs.sh "tune_pm_"$sample_fraction"_"$k"_"$tau_init"_200topics" $submit_file
		done
	done
done
