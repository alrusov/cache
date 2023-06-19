package cache

import (
	"sort"
	"sync"
	"time"

	"github.com/alrusov/config"
	"github.com/alrusov/initializer"
	"github.com/alrusov/jsonw"
	"github.com/alrusov/log"
	"github.com/alrusov/misc"
)

//----------------------------------------------------------------------------------------------------------------------------//

type (
	Cache struct {
		mutex *sync.Mutex
		data  Elems
	}

	Elems map[string]*Elem

	Elem struct {
		def
		cond  *sync.Cond // Для ожидания первого заполнения
		cache *Cache     // Ссылка на кеш
		Data  any        `json:"-"` // Данные
	}

	Stats []Stat

	Stat struct {
		def
	}

	def struct {
		Key             string          `json:"key"`             // Ключ
		Description     string          `json:"description"`     // Дополнительное описание для визуализации
		Hash            string          `json:"hash"`            // hash
		Lifetime        config.Duration `json:"lifetime"`        // lifetime
		CreatedAt       time.Time       `json:"createdAt"`       // Время первоначального создания
		InProgressFrom  time.Time       `json:"inProgressFrom"`  // Время начала обновления
		LastUpdatedAt   time.Time       `json:"lastUpdatedAt"`   // Время последнего обновления
		ExparedAt       time.Time       `json:"exparedAt"`       // Время оуончания жизни
		Filled          bool            `json:"filled"`          // Зполнено актуальными данными
		Code            int             `json:"code"`            // code
		NumberOfUpdates uint            `json:"numberOfUpdates"` // Количество обновлений
		NumberOfUses    uint            `json:"numberOfUses"`    // Количество использований
	}
)

var (
	Log     = log.NewFacility("cache")
	storage *Cache
)

//----------------------------------------------------------------------------------------------------------------------------//

func init() {
	// Регистрируем инициализатор
	initializer.RegisterModuleInitializer(initModule)
}

// Инициализация
func initModule(appCfg any, h any) (err error) {
	storage = New()

	Log.Message(log.INFO, "Initialized")
	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func New() (c *Cache) {
	c = &Cache{
		mutex: new(sync.Mutex),
		data:  make(Elems, 128),
	}

	go c.gc()

	return c
}

//----------------------------------------------------------------------------------------------------------------------------//

func (c *Cache) gc() {
	Log.Message(log.INFO, "gc started")

	for misc.AppStarted() {
		c.mutex.Lock()
		now := misc.NowUTC()

		for hash, e := range c.data {
			if !e.InProgressFrom.IsZero() {
				continue
			}

			if now.Sub(e.LastUpdatedAt) < 2*e.Lifetime.D() {
				continue
			}

			delete(c.data, hash)
		}

		c.mutex.Unlock()
		misc.Sleep(60 * time.Second)
	}

	Log.Message(log.INFO, "gc stopped")
}

//----------------------------------------------------------------------------------------------------------------------------//

func Get(id uint64, key string, description string, extra ...any) (e *Elem, data any, code int) {
	return storage.Get(id, key, description, extra...)
}

func (c *Cache) Get(id uint64, key string, description string, extra ...any) (e *Elem, data any, code int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := misc.NowUTC()

	hash := makeHash(key, extra)
	e, exists := c.data[hash]
	if !exists { // Не существует
		// Создадим новый
		e = &Elem{
			cond:  sync.NewCond(c.mutex),
			cache: c,
			def: def{
				Key:       key,
				Hash:      hash,
				CreatedAt: now,
			},
		}

		c.data[hash] = e
		e.debug(id, "new")

	} else { // Уже существует
		if e.Filled { // Заполнен
			if now.Before(e.ExparedAt) || // Актуален
				!e.InProgressFrom.IsZero() { // или в процессе обновления
				// Берём что дают и уходим
				code = e.Code
				data = e.Data
				e.NumberOfUses++

				e.debug(id, "used")
				e = nil
				return
			}

			// Не актуален и не заполняется, тогда провалимся ниже будем заполнять сами
			e.debug(id, "updating...")

		} else { // Не заполнен
			if !e.InProgressFrom.IsZero() { // В процессе заполнения
				// Будем ждать заполнения
				e.debug(id, "waiting...")
				e.cond.Wait()
				e.debug(id, "resumed")

				// Дождались
				code = e.Code
				data = e.Data
				e.NumberOfUses++
				e = nil
				return
			}

			// Не заполняется, тогда провалимся ниже будем заполнять сами
		}
	}

	// Надо заполнять
	// Вызывающий должен это понять по e != nil, сформировать данные и вызвать e.Commit()

	e.InProgressFrom = now
	e.Description = description

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Данные сформированы, сохраняем
func (e *Elem) Commit(id uint64, data any, code int, lifetime config.Duration) {
	e.cache.mutex.Lock()
	defer e.cache.mutex.Unlock()

	e.InProgressFrom = time.Time{}
	e.LastUpdatedAt = misc.NowUTC()
	e.Lifetime = lifetime
	e.ExparedAt = e.LastUpdatedAt.Add(lifetime.D())
	e.Filled = true
	e.Code = code
	e.Data = data
	e.NumberOfUpdates++
	e.NumberOfUses++

	e.cond.Broadcast()

	e.debug(id, "commited")
}

//----------------------------------------------------------------------------------------------------------------------------//

func makeHash(key string, extra ...any) (hash string) {
	d := struct {
		Key   string
		Extra []any
	}{
		Key:   key,
		Extra: extra,
	}

	j, _ := jsonw.Marshal(d)
	hash = string(misc.Sha512Hash(j))
	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (e *Elem) debug(id uint64, op string) {
	if Log.CurrentLogLevel() >= log.DEBUG {
		j, _ := jsonw.Marshal(e)
		Log.Message(log.DEBUG, "[%d] %s %s", id, op, j)
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

func GetStat() (s Stats) {
	return storage.GetStat()
}

func (c *Cache) GetStat() (s Stats) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	s = make(Stats, 0, len(c.data))

	for _, e := range c.data {
		s = append(s,
			Stat{
				def: e.def,
			},
		)
	}

	sort.Sort(s)
	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (s Stats) Len() int {
	return len(s)
}

func (s Stats) Less(i, j int) bool {
	if s[i].Key == s[j].Key {
		return s[i].Description < s[j].Description
	}

	return s[i].Key < s[j].Key
}

func (s Stats) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

//----------------------------------------------------------------------------------------------------------------------------//
