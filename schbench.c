/*
 * schbench.c
 *
 * Copyright (C) 2016 Facebook
 * Chris Mason <clm@fb.com>
 *
 * GPLv2, portions copied from the kernel and from Jens Axboe's fio
 *
 * gcc -Wall -O0 -W schbench.c -o schbench -lpthread
 */
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <getopt.h>
#include <sys/time.h>
#include <time.h>
#include <string.h>
#include <linux/futex.h>
#include <sys/syscall.h>

#define PLAT_BITS	8
#define PLAT_VAL	(1 << PLAT_BITS)
#define PLAT_GROUP_NR	19
#define PLAT_NR		(PLAT_GROUP_NR * PLAT_VAL)
#define PLAT_LIST_MAX	20

/* when -p is on, how much do we send back and forth */
#define PIPE_TRANSFER_BUFFER (1 * 1024 * 1024)

/* -m number of message threads */
static int message_threads = 2;
/* -t  number of workers per message thread */
static int worker_threads = 16;
/* -r  seconds */
static int runtime = 30;
/* -s  usec */
static int sleeptime = 30000;
/* -c  usec */
static unsigned long long cputime = 30000;
/* -a, bool */
static int autobench = 0;
/* -p bytes */
static int pipe_test = 0;

/* the message threads flip this to true when they decide runtime is up */
static volatile unsigned long stopping = 0;


/*
 * one stat struct per thread data, when the workers sleep this records the
 * latency between when they are woken up and when they actually get the
 * CPU again.  The message threads sum up the stats of all the workers and
 * then bubble them up to main() for printing
 */
struct stats {
	unsigned int plat[PLAT_NR];
	unsigned int nr_samples;
	unsigned int max;
	unsigned int min;
};

/* this defines which latency profiles get printed */
#define PLIST_P99 4
static double plist[PLAT_LIST_MAX] = { 50.0, 75.0, 90.0, 95.0, 99.0, 99.5, 99.9 };

enum {
	HELP_LONG_OPT = 1,
};

char *option_string = "p:am:t:s:c:r:";
static struct option long_options[] = {
	{"auto", no_argument, 0, 'a'},
	{"pipe", required_argument, 0, 'p'},
	{"message-threads", required_argument, 0, 'm'},
	{"threads", required_argument, 0, 't'},
	{"runtime", required_argument, 0, 'r'},
	{"sleeptime", required_argument, 0, 's'},
	{"cputime", required_argument, 0, 'c'},
	{"help", no_argument, 0, HELP_LONG_OPT},
	{0, 0, 0, 0}
};

static void print_usage(void)
{
	fprintf(stderr, "schbench usage:\n"
		"\t-m (--message-threads): number of message threads (def: 2)\n"
		"\t-t (--threads): worker threads per message thread (def: 16)\n"
		"\t-r (--runtime): How long to run before exiting (seconds, def: 30)\n"
		"\t-s (--sleeptime): Message thread latency (usec, def: 10000\n"
		"\t-c (--cputime): How long to think during loop (usec, def: 10000\n"
		"\t-a (--auto): grow thread count until latencies hurt (def: off)\n"
		"\t-p (--pipe): transfer size bytes to simulate a pipe test (def: 0)\n"
	       );
	exit(1);
}

static void parse_options(int ac, char **av)
{
	int c;
	int found_sleeptime = -1;
	int found_cputime = -1;

	while (1) {
		int option_index = 0;

		c = getopt_long(ac, av, option_string,
				long_options, &option_index);

		if (c == -1)
			break;

		switch(c) {
		case 'a':
			autobench = 1;
			break;
		case 'p':
			pipe_test = atoi(optarg);
			if (pipe_test > PIPE_TRANSFER_BUFFER) {
				fprintf(stderr, "pipe size too big, using %d\n",
					PIPE_TRANSFER_BUFFER);
				pipe_test = PIPE_TRANSFER_BUFFER;
			}
			sleeptime = 0;
			cputime = 0;
			break;
		case 's':
			found_sleeptime = atoi(optarg);
			break;
		case 'c':
			found_cputime = atoi(optarg);
			break;
		case 'm':
			message_threads = atoi(optarg);
			break;
		case 't':
			worker_threads = atoi(optarg);
			break;
		case 'r':
			runtime = atoi(optarg);
			break;
		case '?':
		case HELP_LONG_OPT:
			print_usage();
			break;
		default:
			break;
		}
	}

	/*
	 * by default pipe mode zeros out cputime and sleep time.  This
	 * sets them to any args that were actually passed in
	 */
	if (found_sleeptime >= 0)
		sleeptime = found_sleeptime;
	if (found_cputime >= 0)
		cputime = found_cputime;

	if (optind < ac) {
		fprintf(stderr, "Error Extra arguments '%s'\n", av[optind]);
		exit(1);
	}
}

void tvsub(struct timeval * tdiff, struct timeval * t1, struct timeval * t0)
{
	tdiff->tv_sec = t1->tv_sec - t0->tv_sec;
	tdiff->tv_usec = t1->tv_usec - t0->tv_usec;
	if (tdiff->tv_usec < 0 && tdiff->tv_sec > 0) {
		tdiff->tv_sec--;
		tdiff->tv_usec += 1000000;
		if (tdiff->tv_usec < 0) {
			fprintf(stderr, "lat_fs: tvsub shows test time ran backwards!\n");
			exit(1);
		}
	}

	/* time shouldn't go backwards!!! */
	if (tdiff->tv_usec < 0 || t1->tv_sec < t0->tv_sec) {
		tdiff->tv_sec = 0;
		tdiff->tv_usec = 0;
	}
}

/*
 * returns the difference between start and stop in usecs.  Negative values
 * are turned into 0
 */
unsigned long long tvdelta(struct timeval *start, struct timeval *stop)
{
	struct timeval td;
	unsigned long long usecs;

	tvsub(&td, stop, start);
	usecs = td.tv_sec;
	usecs *= 1000000;
	usecs += td.tv_usec;
	return (usecs);
}

/* mr axboe's magic latency histogram */
static unsigned int plat_val_to_idx(unsigned int val)
{
	unsigned int msb, error_bits, base, offset;

	/* Find MSB starting from bit 0 */
	if (val == 0)
		msb = 0;
	else
		msb = sizeof(val)*8 - __builtin_clz(val) - 1;

	/*
	 * MSB <= (PLAT_BITS-1), cannot be rounded off. Use
	 * all bits of the sample as index
	 */
	if (msb <= PLAT_BITS)
		return val;

	/* Compute the number of error bits to discard*/
	error_bits = msb - PLAT_BITS;

	/* Compute the number of buckets before the group */
	base = (error_bits + 1) << PLAT_BITS;

	/*
	 * Discard the error bits and apply the mask to find the
	 * index for the buckets in the group
	 */
	offset = (PLAT_VAL - 1) & (val >> error_bits);

	/* Make sure the index does not exceed (array size - 1) */
	return (base + offset) < (PLAT_NR - 1) ?
		(base + offset) : (PLAT_NR - 1);
}

/*
 * Convert the given index of the bucket array to the value
 * represented by the bucket
 */
static unsigned int plat_idx_to_val(unsigned int idx)
{
	unsigned int error_bits, k, base;

	if (idx >= PLAT_NR) {
		fprintf(stderr, "idx %u is too large\n", idx);
		exit(1);
	}

	/* MSB <= (PLAT_BITS-1), cannot be rounded off. Use
	 * all bits of the sample as index */
	if (idx < (PLAT_VAL << 1))
		return idx;

	/* Find the group and compute the minimum value of that group */
	error_bits = (idx >> PLAT_BITS) - 1;
	base = 1 << (error_bits + PLAT_BITS);

	/* Find its bucket number of the group */
	k = idx % PLAT_VAL;

	/* Return the mean of the range of the bucket */
	return base + ((k + 0.5) * (1 << error_bits));
}


static unsigned int calc_percentiles(unsigned int *io_u_plat, unsigned long nr,
				     unsigned int **output)
{
	unsigned long sum = 0;
	unsigned int len, i, j = 0;
	unsigned int oval_len = 0;
	unsigned int *ovals = NULL;
	int is_last;

	len = 0;
	while (len < PLAT_LIST_MAX && plist[len] != 0.0)
		len++;

	if (!len)
		return 0;

	/*
	 * Calculate bucket values, note down max and min values
	 */
	is_last = 0;
	for (i = 0; i < PLAT_NR && !is_last; i++) {
		sum += io_u_plat[i];
		while (sum >= (plist[j] / 100.0 * nr)) {
			if (j == oval_len) {
				oval_len += 100;
				ovals = realloc(ovals, oval_len * sizeof(unsigned int));
			}

			ovals[j] = plat_idx_to_val(i);
			is_last = (j == len - 1);
			if (is_last)
				break;

			j++;
		}
	}

	*output = ovals;
	return len;
}

static int calc_p99(struct stats *s)
{
	unsigned int *ovals = NULL;
	int ret = 0;
	int len;

	len = calc_percentiles(s->plat, s->nr_samples, &ovals);
	if (len && len > PLIST_P99)
		ret = ovals[PLIST_P99];
	if (ovals)
		free(ovals);
	return ret;
}

static void show_latencies(struct stats *s)
{
	unsigned int *ovals = NULL;
	unsigned int len, i;

	len = calc_percentiles(s->plat, s->nr_samples, &ovals);
	if (len) {
		fprintf(stderr, "Latency percentiles (usec)\n");
		for (i = 0; i < len; i++)
			fprintf(stderr, "\t%s%2.4fth: %u\n",
				i == PLIST_P99 ? "*" : "",
				plist[i], ovals[i]);
	}

	if (ovals)
		free(ovals);

	fprintf(stderr, "\tmin=%u, max=%u\n", s->min, s->max);
}

/* fold latency info from s into d */
void combine_stats(struct stats *d, struct stats *s)
{
	int i;
	for (i = 0; i < PLAT_NR; i++)
		d->plat[i] += s->plat[i];
	d->nr_samples += s->nr_samples;
	if (s->max > d->max)
		d->max = s->max;
	if (s->min < d->min)
		d->min = s->min;
}

/* record a latency result into the histogram */
static void add_lat(struct stats *s, unsigned int us)
{
	int lat_index = 0;

	if (us > s->max)
		s->max = us;
	if (us < s->min)
		s->min = us;

	lat_index = plat_val_to_idx(us);
	__sync_fetch_and_add(&s->plat[lat_index], 1);
	__sync_fetch_and_add(&s->nr_samples, 1);
}

/*
 * every thread has one of these, it comes out to about 19K thanks to the
 * giant stats struct
 */
struct thread_data {
	pthread_t tid;
	/* ->next is for placing us on the msg_thread's list for waking */
	struct thread_data *next;

	/* our parent thread and messaging partner */
	struct thread_data *msg_thread;

	/*
	 * the msg thread stuffs gtod in here before waking us, so we can
	 * measure scheduler latency
	 */
	struct timeval wake_time;

	/* keep the futex and the wake_time in the same cacheline */
	int futex;

	/* mr axboe's magic latency histogram */
	struct stats stats;
	double loops_per_sec;
	char pipe_page[PIPE_TRANSFER_BUFFER];
};

/* we're so fancy we make our own futex wrappers */
#define FUTEX_BLOCKED 0
#define FUTEX_RUNNING 1

static int futex(int *uaddr, int futex_op, int val,
		 const struct timespec *timeout, int *uaddr2, int val3)
{
	return syscall(SYS_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
}

/*
 * wakeup a process waiting on a futex, making sure they are really waiting
 * first
 */
static void fpost(int *futexp)
{
	int s;

	if (__sync_bool_compare_and_swap(futexp, FUTEX_BLOCKED,
					 FUTEX_RUNNING)) {
		s = futex(futexp, FUTEX_WAKE_PRIVATE, 1, NULL, NULL, 0);
		if (s  == -1) {
			perror("FUTEX_WAKE");
			exit(1);
		}
	}
}

/*
 * wait on a futex, with an optional timeout.  Make sure to set
 * the futex to FUTEX_BLOCKED beforehand.
 *
 * This will return zero if all went well, or return -ETIMEDOUT if you
 * hit the timeout without getting posted
 */
static int fwait(int *futexp, struct timespec *timeout)
{
	int s;
	while (1) {
		/* Is the futex available? */
		if (__sync_bool_compare_and_swap(futexp, FUTEX_RUNNING,
						 FUTEX_BLOCKED)) {
			break;      /* Yes */
		}
		/* Futex is not available; wait */
		s = futex(futexp, FUTEX_WAIT_PRIVATE, FUTEX_BLOCKED, timeout, NULL, 0);
		if (s == -1 && errno != EAGAIN) {
			if (errno == ETIMEDOUT)
				return -ETIMEDOUT;
			perror("futex-FUTEX_WAIT");
			exit(1);
		}
	}
	return 0;
}

/*
 * cmpxchg based list prepend
 */
static void xlist_add(struct thread_data *head, struct thread_data *add)
{
	struct thread_data *old;
	struct thread_data *ret;

	while (1) {
		old = head->next;
		add->next = old;
		ret = __sync_val_compare_and_swap(&head->next, old, add);
		if (ret == old)
			break;
	}
}

/*
 * xchg based list splicing.  This returns the entire list and
 * replaces the head->next with NULL
 */
static struct thread_data *xlist_splice(struct thread_data *head)
{
	struct thread_data *old;
	struct thread_data *ret;

	while (1) {
		old = head->next;
		ret = __sync_val_compare_and_swap(&head->next, old, NULL);
		if (ret == old)
			break;
	}
	return ret;
}

/*
 * Wake everyone currently waiting on the message list, filling in their
 * thread_data->wake_time with the current time.
 *
 * It's not exactly the current time, it's really the time at the start of
 * the list run.  We want to detect when the scheduler is just preempting the
 * waker and giving away the rest of its timeslice.  So we gtod once at
 * the start of the loop and use that for all the threads we wake.
 *
 * Since pipe mode ends up measuring this other ways, we do the gtod
 * every time in pipe mode
 */
static void xlist_wake_all(struct thread_data *td)
{
	struct thread_data *list;
	struct thread_data *next;
	struct timeval now;

	list = xlist_splice(td);
	gettimeofday(&now, NULL);
	while (list) {
		next = list->next;
		list->next = NULL;
		if (pipe_test) {
			memset(list->pipe_page, 1, pipe_test);
			gettimeofday(&list->wake_time, NULL);
		} else {
			memcpy(&list->wake_time, &now, sizeof(now));
		}
		fpost(&list->futex);
		list = next;
	}
}

/*
 * called by worker threads to send a message and wait for the answer.
 * In reality we're just trading one cacheline with the gtod and futex in
 * it, but that's good enough.  We gtod after waking and use that to
 * record scheduler latency.
 */
static void msg_and_wait(struct thread_data *td)
{
	struct timeval now;
	unsigned long long delta;

	if (pipe_test)
		memset(td->pipe_page, 2, pipe_test);

	/* set ourselves to blocked */
	td->futex = FUTEX_BLOCKED;
	gettimeofday(&td->wake_time, NULL);

	/* add us to the list */
	xlist_add(td->msg_thread, td);

	fpost(&td->msg_thread->futex);

	/*
	 * don't wait if the main threads are shutting down,
	 * they will never kick us fpost has a full barrier, so as long
	 * as the message thread walks his list after setting stopping,
	 * we shouldn't miss the wakeup
	 */
	if (!stopping) {
		/* if he hasn't already woken us up, wait */
		fwait(&td->futex, NULL);
	}

	gettimeofday(&now, NULL);
	delta = tvdelta(&td->wake_time, &now);
	if (delta > 0)
		add_lat(&td->stats, delta);
}

/*
 * once the message thread starts all his children, this is where he
 * loops until our runtime is up.  Basically this sits around waiting
 * for posting by the worker threads, replying to their messages after
 * a delay of 'sleeptime' + some jitter.
 */
static void run_msg_thread(struct thread_data *td)
{
	unsigned int seed = pthread_self();
	int max_jitter = sleeptime / 4;
	int jitter = 0;

	while (1) {
		td->futex = FUTEX_BLOCKED;
		xlist_wake_all(td);

		if (stopping) {
			/*
			 * add a barrier to make sure everyone has seen
			 * stopping.  xlist_wake_all does barrier,
			 * but it doesn't loop
			 */
			__sync_synchronize();
			xlist_wake_all(td);
			break;
		}
		fwait(&td->futex, NULL);

		/*
		 * messages shouldn't be instant, sleep a little to make them
		 * wait
		 */
		if (!pipe_test && sleeptime) {
			jitter = rand_r(&seed) % max_jitter;
			usleep(sleeptime + jitter);
		}
	}
}

#define nop __asm__ __volatile__("rep;nop": : :"memory")

static void usec_spin(unsigned long spin_time)
{
	struct timeval now;
	struct timeval start;
	unsigned long long delta;

	if (spin_time == 0)
		return;

	gettimeofday(&start, NULL);
	while (1) {
		gettimeofday(&now, NULL);
		delta = tvdelta(&start, &now);
		if (delta > spin_time)
			return;
		nop;
	}
}

/*
 * the worker thread is pretty simple, it just does a single spin and
 * then waits on a message from the message thread
 */
void *worker_thread(void *arg)
{
	struct thread_data *td = arg;
	struct timeval now;
	struct timeval start;
	unsigned long long delta;
	unsigned long loop_count = 0;
	double seconds;

	gettimeofday(&start, NULL);
	while(1) {
		if (stopping)
			break;

		usec_spin(cputime);
		msg_and_wait(td);
		loop_count++;
	}
	gettimeofday(&now, NULL);
	delta = tvdelta(&start, &now);

	seconds = (double)delta/1000000;
	td->loops_per_sec = (double)loop_count / seconds;
	return NULL;
}

/*
 * the message thread starts his own gaggle of workers and then sits around
 * replying when they post him.  He collects latency stats as all the threads
 * exit
 */
void *message_thread(void *arg)
{
	struct thread_data *td = arg;
	struct thread_data *worker_threads_mem = NULL;
	int i;
	int ret;

	worker_threads_mem = calloc(worker_threads, sizeof(struct thread_data));

	if (!worker_threads_mem) {
		perror("unable to allocate ram");
		pthread_exit((void *)-ENOMEM);
	}

	for (i = 0; i < worker_threads; i++) {
		pthread_t tid;

		worker_threads_mem[i].msg_thread = td;
		ret = pthread_create(&tid, NULL, worker_thread,
				     worker_threads_mem + i);
		if (ret) {
			fprintf(stderr, "error %d from pthread_create\n", ret);
			exit(1);
		}
		worker_threads_mem[i].tid = tid;
	}

	run_msg_thread(td);

	for (i = 0; i < worker_threads; i++) {
		fpost(&worker_threads_mem[i].futex);
		pthread_join(worker_threads_mem[i].tid, NULL);
		combine_stats(&td->stats, &worker_threads_mem[i].stats);
		td->loops_per_sec += worker_threads_mem[i].loops_per_sec;
	}
	free(worker_threads_mem);

	td->loops_per_sec /= worker_threads;
	return NULL;
}

static char *units[] = { "B", "KB", "MB", "GB", "TB", "PB", "EB", NULL};

static double pretty_size(double number, char **str)
{
	int divs = 0;

	while(number >= 1024) {
		if (units[divs + 1] == NULL)
			break;
		divs++;
		number /= 1024;
	}
	*str = units[divs];
	return number;
}

/* runtime from the command line is in seconds.  Sleep until its up */
static void sleep_for_runtime()
{
	struct timeval now;
	struct timeval start;
	unsigned long long delta;
	unsigned long long runtime_usec = runtime * 1000000;

	gettimeofday(&start, NULL);
	sleep(runtime);

	while(1) {
		gettimeofday(&now, NULL);
		delta = tvdelta(&start, &now);
		if (delta < runtime_usec)
			sleep(1);
		else
			break;
	}
	__sync_synchronize();
	stopping = 1;
}

int main(int ac, char **av)
{
	int i;
	int ret;
	struct thread_data *message_threads_mem = NULL;
	struct stats stats;
	double loops_per_sec = 0;

	parse_options(ac, av);
again:
	stopping = 0;
	memset(&stats, 0, sizeof(stats));

	message_threads_mem = calloc(message_threads,
				      sizeof(struct thread_data));


	if (!message_threads_mem) {
		perror("unable to allocate message threads");
		exit(1);
	}

	/* start our message threads, each one starts its own workers */
	for (i = 0; i < message_threads; i++) {
		pthread_t tid;
		ret = pthread_create(&tid, NULL, message_thread,
				     message_threads_mem + i);
		if (ret) {
			fprintf(stderr, "error %d from pthread_create\n", ret);
			exit(1);
		}
		message_threads_mem[i].tid = tid;
	}

	sleep_for_runtime();

	for (i = 0; i < message_threads; i++) {
		fpost(&message_threads_mem[i].futex);
		pthread_join(message_threads_mem[i].tid, NULL);
		combine_stats(&stats, &message_threads_mem[i].stats);
		loops_per_sec += message_threads_mem[i].loops_per_sec;
	}

	free(message_threads_mem);

	/*
	 * in auto bench mode, keep adding workers until our latencies get
	 * horrible
	 */
	if (autobench) {
		int p99 = calc_p99(&stats);
		fprintf(stderr, "cputime %Lu threads %d p99 %d\n",
			cputime, worker_threads, p99);
		if (p99 < 2000) {
			worker_threads++;
			goto again;
		}
	}

	show_latencies(&stats);
	if (pipe_test) {
		char *pretty;
		double mb_per_sec;
		loops_per_sec /= message_threads;
		mb_per_sec = loops_per_sec * pipe_test;
		mb_per_sec = pretty_size(mb_per_sec, &pretty);
		printf("avg worker transfer: %.2f ops/sec %.2f%s/s\n",
		       loops_per_sec, mb_per_sec, pretty);

	}

	return 0;
}
