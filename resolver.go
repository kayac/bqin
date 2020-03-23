package bqin

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/kayac/bqin/internal/logger"
)

type Resolver struct {
	rules []*Rule
}

func NewResolver(rules []*Rule) *Resolver {
	return &Resolver{
		rules: rules,
	}
}

func (r *Resolver) Resolve(urls []*url.URL) []*Job {
	ret := make([]*Job, 0, len(urls))
	for _, u := range urls {
		logger.Debugf("check url :%s", u.String())
		for _, rule := range r.rules {
			ok, capture := rule.Match(u)
			if !ok {
				continue
			}
			logger.Debugf("match rule: %s", rule.String())
			ret = append(ret, newJob(rule, u, capture))
		}
	}
	return ret
}

type Job struct {
	*TransportJob
	*LoadingJob
}

func newJob(r *Rule, u *url.URL, capture []string) *Job {
	temp := &url.URL{
		Scheme: "gs",
		Host:   expandPlaceHolder(r.Option.TemporaryBucket, capture),
		Path:   u.Path,
	}
	dest := &LoadingDestination{
		ProjectID: expandPlaceHolder(r.BigQuery.ProjectID, capture),
		Dataset:   expandPlaceHolder(r.BigQuery.Dataset, capture),
		Table:     expandPlaceHolder(r.BigQuery.Table, capture),
	}
	loadingJob := NewLoadingJob(dest, temp.String())
	loadingJob.GCSRef.Compression = r.Option.getCompression()
	loadingJob.GCSRef.AutoDetect = r.Option.getAutoDetect()
	loadingJob.GCSRef.SourceFormat = r.Option.getSourceFormat()

	return &Job{
		TransportJob: &TransportJob{
			Source:      u,
			Destination: temp,
		},
		LoadingJob: loadingJob,
	}
}

// example: when capture []string{"hoge"},  table_$1 => table_hoge
func expandPlaceHolder(s string, capture []string) string {
	for i, v := range capture {
		s = strings.Replace(s, "$"+strconv.Itoa(i), v, -1)
	}
	return s
}
