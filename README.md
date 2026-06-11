<img src="https://cdn.prod.website-files.com/677c400686e724409a5a7409/6790ad949cf622dc8dcd9fe4_nextwork-logo-leather.svg" alt="NextWork" width="300" />

# Distributed Task Queue with FastAPI

**Project Link:** [View Project](https://learn.nextwork.org/projects/39668a32-07c3-4783-876e-d0ba871868b2)

**Author:** Mayan Sequeira  
**Email:** mayan.sequeira@gmail.com

---

![Image](https://learn.nextwork.org/grateful_magenta_fierce_white_currant/uploads/39668a32-07c3-4783-876e-d0ba871868b2_ux2zyodv)

## Distributed Task Queue: Architecture and Vision

### Project overview and goals

In this step, I'm setting up the dependencies for the project... so that I can run the project in the custom environment...

![Image](https://learn.nextwork.org/grateful_magenta_fierce_white_currant/uploads/39668a32-07c3-4783-876e-d0ba871868b2_2grz3ych)

### Verifying Docker Compose installation

I ran docker compose version... and it returned version...Docker Compose version v5.1.0

## Building the FastAPI REST Layer

### Designing the API endpoints

In this step, I'm building a web framework using FastAPI to call APIs that give job requests to be processed... so that clients don't need to wait...

![Image](https://learn.nextwork.org/grateful_magenta_fierce_white_currant/uploads/39668a32-07c3-4783-876e-d0ba871868b2_bor4sadb)

### Async response design with 202 Accepted

The endpoint returns a status code of 202... because Celery tasks have successfully received the request and don't need the client to wait for the response for it...

## Implementing Production Reliability Patterns in the Celery Worker

### Worker architecture and goals

In this step, I'm building relaibitlty on celery workers... so that the system can execute the tasks given so it does not die of starvation...

### Retry logic, exponential backoff, and idempotency

The three patterns I implemented are exponential backoff, idempotent execution, and retry logic... They matter because the worker could fail or delay execution so we need to smoothen the process...

## Containerizing a Multi-Service System with Docker Compose

### Orchestrating API, worker, and broker containers

In this step, I'm containerizing the microservices I have used, like Redis broker, Celery worker, and APIs... so that I can pack them up in a container for shipping it to test and production environments...

![Image](https://learn.nextwork.org/grateful_magenta_fierce_white_currant/uploads/39668a32-07c3-4783-876e-d0ba871868b2_nn6l00zq)

### End-to-end job flow across containers

When I submit a job, the API routes it to the Redis broker and keeps it in the task queue... then Redis stores the tasks waiting for the consumer... then the worker waiting for the tasks immediately consumes the task present in the queue and executes it...

## Real-Time Observability with the Flower Monitoring Dashboard

### Adding visibility into the distributed system

In this step, I'm exploring flower dashboard... so that I can see how healthy the system is in production...

![Image](https://learn.nextwork.org/grateful_magenta_fierce_white_currant/uploads/39668a32-07c3-4783-876e-d0ba871868b2_ux2zyodv)

### Insights from the Flower dashboard

The Flower dashboard shows me the status and history of the workers, tasks and brokers queue.

## Priority Queues and Dedicated Worker Routing

![Image](https://learn.nextwork.org/grateful_magenta_fierce_white_currant/uploads/39668a32-07c3-4783-876e-d0ba871868b2_sqx27gm2)

### Why priority routing outperforms a single FIFO queue

In this project extension, priority routing improves the system because it gives attention to the high-priority tasks that needs to be solved soon without delay...

---

*Built with [NextWork](https://learn.nextwork.org) - [View this project](https://learn.nextwork.org/projects/39668a32-07c3-4783-876e-d0ba871868b2)*
