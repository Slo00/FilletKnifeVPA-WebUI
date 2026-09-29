# FilletKnifeVPA UI

Веб-интерфейс к VPA-recommender'у на VictoriaMetrics
([Slo00/FilletKnifeVPA](https://github.com/Slo00/FilletKnifeVPA)).

Для каждого VPA показывает рядом:

- requests/limits из манифеста workload (Deployment/StatefulSet/…, Helm-релиз);
- что реально стоит на подах;
- рекомендацию VPA с коридором и расчёт «сейчас» — тем же кодом, что у recommender'а;
- текущее потребление и графики usage/requests/limits из VictoriaMetrics.

Позволяет закрепить свои значения, заморозить рекомендацию и вернуть автопилот.
Оверрайд пишется аннотациями на VPA (кто, когда, срок, причина) и событием Kubernetes,
применяет его recommender. До нажатия кнопки страница показывает, будут ли перезапущены поды.

Своей БД и логина нет: всё состояние в кластере, автор берётся из заголовка SSO-прокси
(`--user-header`, по умолчанию `X-Forwarded-User`).

## Зависимость от форка VPA

Пакеты recommender'а (`victoriametrics`, `override`, `config`) берутся из форка.
В `go.mod` модуль `k8s.io/autoscaler/vertical-pod-autoscaler` подменён через `replace`:

- для локальной разработки — на соседнюю папку `../FilletKnifeVPA/vertical-pod-autoscaler`;
- для сборки образа и CI — на коммит форка на GitHub:

```bash
go mod edit -replace=k8s.io/autoscaler/vertical-pod-autoscaler=github.com/Slo00/FilletKnifeVPA/vertical-pod-autoscaler@<commit>
go mod tidy
```

## Запуск локально

```bash
kubectl -n monitoring port-forward svc/vm-victoria-metrics-single-server 8428:8428 &
go run . \
  --kubeconfig=$HOME/.kube/config \
  --listen=127.0.0.1:8090 --dev-user=<имя> \
  --prometheus-address=http://localhost:8428 \
  --prometheus-cadvisor-job-name=kubernetes-nodes-cadvisor \
  --history-length=1d --history-resolution=1m
```

Флаги VictoriaMetrics — те же, что у recommender'а, иначе «расчёт сейчас» не совпадёт
с тем, что он публикует. `--dev-user` подставляет автора вместо заголовка SSO-прокси,
только для локального запуска.

## Образ и установка

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t <registry>/vpa-ui:<tag> --push .
kubectl apply -f deploy/vpa-ui.yaml
```

`deploy/vpa-ui.yaml` — ServiceAccount, минимальный ClusterRole (чтение VPA, workload, подов;
patch VPA; создание событий), Deployment и Service. Страница должна быть доступна только через
SSO-прокси — см. комментарий в начале манифеста.
