
gen_param <- function(N, dt) {
    fi_1 = -pi/2 + pi*runif(1,0,1)
    fi_2 = -pi/2 + pi*runif(1,0,1)
    fi_3 = -pi/2 + pi*runif(1,0,1)

    eps1 = 0.9+0.1*runif(1,0,1)
    eps2 = 0.9+0.1*runif(1,0,1)
    eps3 = 0.9+0.1*runif(1,0,1)

    f_1 = (1/(N*dt))*(1+2*runif(1,0,1))
    f_2 = (1/(N*dt))*(1+2*runif(1,0,1))
    f_3 = (1/(N*dt))*(1+2*runif(1,0,1))

    t = (1:N)*dt

    y = eps1*sin(2*pi*f_1*t+fi_1)+eps2*sin(2*pi*f_2*t+fi_2)+eps3*sin(2*pi*f_3*t+fi_3)

    supp = max(y)
    inff = min(y)
    nlevel =(supp-inff)/10
    err = 0.0002

    y_t = eps1*sin(2*pi*f_1*t+fi_1)+eps2*sin(2*pi*f_2*t+fi_2)+eps3*sin(2*pi*(f_3+err)*t+fi_3)

    y1 = y_t+nlevel*rnorm(N, mean = 0, sd = 1)
    
    return(cbind(y, y1))
}

generate_single <- function(params_vector, N, dt, file="scenario.csv", dfile="sim_measur.csv") {
    params = length(params_vector)
    ideal = matrix(nrow=N, ncol=params)
    distorted = matrix(nrow=N, ncol=params)
    for (i in 1:params) {
	generated = gen_param(N, dt)
	ideal[,i] = generated[,1]
	distorted[,i] = generated[,2]
    }
    ideal = rbind(params_vector,ideal)
    distorted = rbind(params_vector, distorted)
    write.table(ideal, file = file, sep=", ", col.names = F, row.names = F)
    write.table(distorted, file = dfile, sep=", ", col.names = F, row.names = F)
}

gen_scenarios <- function(scenarios, directory='data')  {
    unlink(directory, recursive=TRUE)
    dir.create(directory)
    for (i in 1:scenarios) {
	file = paste(c(directory, '/scenario',i,'.csv'),collapse="")
	dfile = paste(c(directory, '/sim_measur',i,'.csv'),collapse="")
	generate_single(params_vector, N, dt, file, dfile)
    }
}

N = 1000 
dt = 0.25 
params_vector = (1:5)

gen_scenarios(1000)

#generate_single(params, N, dt)

